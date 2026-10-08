package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var payload = func() []byte {
	b := make([]byte, 4096)
	for i := range b {
		b[i] = 'x'
	}
	return b
}()

// fakeServer serves payload for every path; failPath, if set, returns 500 for that path.
func fakeServer(t *testing.T, delay time.Duration, failPath string) (string, *int64) {
	t.Helper()
	var hits int64
	corrupt := map[string]bool{}
	for _, p := range strings.Split(os.Getenv("TEST_CORRUPT"), ",") {
		if p != "" {
			corrupt[p] = true
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		if failPath != "" && strings.Contains(r.URL.Path, failPath) {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		body := payload
		for p := range corrupt {
			if strings.Contains(r.URL.Path, p) {
				body = make([]byte, len(payload))
				for i := range body {
					body[i] = 'z'
				}
				break
			}
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/", &hits
}

func testGame() game {
	g := game{Folder: "f"}
	for i := 0; i < 5; i++ {
		g.Files = append(g.Files, fileInfo{
			P: fmt.Sprintf("dir%d/file%d.txt", i, i),
			S: int64(len(payload)),
			H: hashBytes(payload),
			C: "base",
		})
	}
	return g
}

// oneFileGame is a single-file game, for tests that only care about one file.
func oneFileGame() game {
	return game{Folder: "f", Files: []fileInfo{{
		P: "a.txt",
		S: int64(len(payload)),
		H: hashBytes(payload),
	}}}
}

// TestHashGolden pins the little-endian byte-dump encoding the manifest uses.
func TestHashGolden(t *testing.T) {
	cases := []struct {
		content string
		want    string
	}{
		{" ", "CFB73C10FC3715FC"},     // cod4/temp.txt
		{"10190", "A582EF4BCFB08127"}, // mw2/steam_appid.txt
	}
	for _, c := range cases {
		if got := hashBytes([]byte(c.content)); got != c.want {
			t.Errorf("hashBytes(%q) = %s, want %s", c.content, got, c.want)
		}
	}
}

func TestFormatHashIsLittleEndian(t *testing.T) {
	sum := "012C50BC6D7A40EB"
	if formatHash(0xEB407A6DBC502C01) != sum {
		t.Fatalf("formatHash produced %s, want %s", formatHash(0xEB407A6DBC502C01), sum)
	}
	if got := fmt.Sprintf("%016X", uint64(0xEB407A6DBC502C01)); got == sum {
		t.Fatal("test is vacuous: %%016X unexpectedly matches")
	}
}

func TestDownloadSuccess(t *testing.T) {
	base, _ := fakeServer(t, 0, "")
	g := testGame()
	dir := t.TempDir()

	if err := download(context.Background(), g, base, dir, 8, true); err != nil {
		t.Fatalf("download: %v", err)
	}
	for _, f := range g.Files {
		target := filepath.Join(dir, filepath.FromSlash(f.P))
		fi, err := os.Stat(target)
		if err != nil {
			t.Fatalf("missing %s: %v", f.P, err)
		}
		if fi.Size() != f.S {
			t.Fatalf("%s size %d, want %d", f.P, fi.Size(), f.S)
		}
	}
}

// TestRestartAllValidSkipsDownload exercises the concurrent verify path: every
// on-disk file already matches, so nothing is fetched from the server.
func TestRestartAllValidSkipsDownload(t *testing.T) {
	base, hits := fakeServer(t, 0, "")
	g := testGame()
	dir := t.TempDir()

	for _, f := range g.Files {
		target := filepath.Join(dir, filepath.FromSlash(f.P))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, payload, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if err := download(context.Background(), g, base, dir, 8, true); err != nil {
		t.Fatalf("download: %v", err)
	}
	if h := atomic.LoadInt64(hits); h != 0 {
		t.Fatalf("want 0 server hits for pre-verified files, got %d", h)
	}
	for _, f := range g.Files {
		target := filepath.Join(dir, filepath.FromSlash(f.P))
		if data, err := os.ReadFile(target); err != nil || string(data) != string(payload) {
			t.Fatalf("%s was modified in place", f.P)
		}
	}
}

// TestDownloadDetectsCorruption is the bug this whole change exists for: a corrupt download must be caught on write.
func TestDownloadDetectsCorruption(t *testing.T) {
	t.Setenv("TEST_CORRUPT", "dir2/file2.txt")
	base, _ := fakeServer(t, 0, "")
	g := testGame()
	dir := t.TempDir()

	err := download(context.Background(), g, base, dir, 8, true)
	if err == nil || !strings.Contains(err.Error(), "file2.txt") {
		t.Fatalf("want error naming file2.txt, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "dir2", "file2.txt")); statErr != nil {
		t.Fatalf("corrupt file should be left in place: %v", statErr)
	}
}

// A size-identical corrupt file already on disk must be caught on restart.
func TestRestartDetectsCorruption(t *testing.T) {
	base, _ := fakeServer(t, 0, "")
	g := testGame()
	dir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(dir, "dir0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "dir1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dir0", "file0.txt"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	bad := make([]byte, len(payload))
	for i := range bad {
		bad[i] = 'q'
	}
	if err := os.WriteFile(filepath.Join(dir, "dir1", "file1.txt"), bad, 0o644); err != nil {
		t.Fatal(err)
	}

	stdin = strings.NewReader("n\n")
	t.Cleanup(func() { stdin = os.Stdin })

	err := download(context.Background(), g, base, dir, 8, true)
	if err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("want failure for corrupt pre-existing file, got %v", err)
	}

	got, hashErr := hashFile(filepath.Join(dir, "dir0", "file0.txt"))
	if hashErr != nil {
		t.Fatal(hashErr)
	}
	if got != g.Files[0].H {
		t.Fatalf("file0 hash %s, want %s", got, g.Files[0].H)
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "dir1", "file1.txt")); data[0] != 'q' {
		t.Fatal("declined file should not have been re-downloaded")
	}
}

func TestDownloadCancel(t *testing.T) {
	base, _ := fakeServer(t, 500*time.Millisecond, "")
	g := testGame()
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	err := download(ctx, g, base, dir, 8, true)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("want interrupted error, got %v", err)
	}
}

func TestAvailabilityCheckFail(t *testing.T) {
	base, _ := fakeServer(t, 0, "dir0/file0.txt")
	g := testGame()
	if err := checkAvailability(context.Background(), g, base); err == nil {
		t.Fatal("want availability error, got nil")
	}
}

// checkAvailability must not pull the whole body just to read a status code.
func TestAvailabilityCheckIsCheap(t *testing.T) {
	var got int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&got, 1)
		w.Write(payload)
	}))
	defer srv.Close()

	g := testGame()
	if err := checkAvailability(context.Background(), g, srv.URL+"/"); err != nil {
		t.Fatalf("availability: %v", err)
	}
	if atomic.LoadInt64(&got) != 1 {
		t.Fatalf("want 1 request, got %d", got)
	}
}

// Transient 5xx should be retried; the run should still succeed.
func TestRetryOnTransientError(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt64(&attempts, 1) == 1 {
			http.Error(w, "transient", http.StatusBadGateway)
			return
		}
		w.Write(payload)
	}))
	defer srv.Close()

	g := oneFileGame()
	dir := t.TempDir()
	if err := download(context.Background(), g, srv.URL+"/", dir, 1, true); err != nil {
		t.Fatalf("want success after retry, got %v", err)
	}
	if atomic.LoadInt64(&attempts) < 2 {
		t.Fatalf("want >=2 attempts, got %d", attempts)
	}
}

// A 404 must NOT be retried — it cannot fix itself.
func TestNoRetryOn404(t *testing.T) {
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		http.NotFound(w, r)
	}))
	defer srv.Close()

	g := oneFileGame()
	dir := t.TempDir()
	_ = download(context.Background(), g, srv.URL+"/", dir, 1, true)
	if n := atomic.LoadInt64(&attempts); n != 1 {
		t.Fatalf("404 should be attempted once, got %d", n)
	}
}

// -keep-going=false restores stop-at-first-failure.
func TestFailFast(t *testing.T) {
	base, _ := fakeServer(t, 0, "dir2/file2.txt")
	g := testGame()
	dir := t.TempDir()

	err := download(context.Background(), g, base, dir, 8, false)
	if err == nil || !strings.Contains(err.Error(), "file2.txt") {
		t.Fatalf("want error mentioning file2.txt, got %v", err)
	}
	if !strings.Contains(err.Error(), "stopped after") {
		t.Fatalf("want fail-fast message, got %v", err)
	}
}

func TestRetryable(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{statusError{500}, true},
		{statusError{503}, true},
		{statusError{404}, false},
		{fmt.Errorf("wrapped: %w", errHashMismatch), false},
		{context.Canceled, false},
		{&os.PathError{Op: "create", Err: os.ErrPermission}, false},
		{errors.New("connection reset by peer"), true},
	}
	for _, c := range cases {
		if got := retryable(c.err); got != c.want {
			t.Errorf("retryable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// The bar must report the true file count, not assume all files finished.
func TestBarStopReportsActualCount(t *testing.T) {
	b := newBar(10, 1000)
	b.commit()
	b.stop(1)
	if b.filesDone != 1 {
		t.Fatalf("filesDone = %d, want 1 (old code hardcoded filesTotal)", b.filesDone)
	}
}

func TestPreskip(t *testing.T) {
	b := newBar(10, 1000)
	b.preskip(400, 4)
	if b.done != 400 || b.filesDone != 4 {
		t.Fatalf("done=%d filesDone=%d, want 400/4", b.done, b.filesDone)
	}
}
