package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zeebo/xxh3"
)

//go:embed games.json
var embeddedData []byte

type fileInfo struct {
	P string `json:"p"`
	S int64  `json:"s"`
	H string `json:"h"`
	C string `json:"c"`
}

type game struct {
	Name   string     `json:"name"`
	Folder string     `json:"folder"`
	Files  []fileInfo `json:"files"`
}

type manifest struct {
	CDNNA string          `json:"cdn_na"`
	CDNEU string          `json:"cdn_eu"`
	Games map[string]game `json:"games"`
}

var m manifest

func main() {
	var (
		region    = flag.String("region", "eu", "CDN region: na or eu")
		want      = flag.String("game", "", "game id, name or number; skips the interactive prompt")
		jobs      = flag.Int("jobs", 8, "max files downloaded simultaneously")
		keepGoing = flag.Bool("keep-going", true, "continue past failing files and report them at the end, instead of stopping at the first failure")
	)
	flag.Parse()

	if err := json.Unmarshal(embeddedData, &m); err != nil {
		fail("failed to parse embedded data: %v", err)
	}

	keys := gameKeys()
	sel := strings.TrimSpace(*want)
	if sel == "" {
		interactive = true
		if !flagSet("region") {
			*region = promptRegion()
		}
		printMenu(keys)
		sel = prompt()
	}

	if *region != "na" && *region != "eu" {
		fail("unknown region %q (use na or eu)", *region)
	}

	var base string
	switch *region {
	case "eu":
		base = m.CDNEU
	case "na":
		base = m.CDNNA
	}

	g, err := resolve(sel, keys)
	if err != nil {
		fail("%v", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		fail("%v", err)
	}

	if *jobs < 1 {
		fail("jobs must be at least 1")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := checkAvailability(ctx, g, base); err != nil {
		fail("availability check failed: %v", err)
	}

	if err := download(ctx, g, base, cwd, *jobs, *keepGoing); err != nil {
		fail("%v", err)
	}
	fmt.Fprintf(os.Stderr, "Finished! Game files are in %s\n", cwd)
	pause()
}

func gameKeys() []string {
	keys := make([]string, 0, len(m.Games))
	for k := range m.Games {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func printMenu(keys []string) {
	fmt.Fprintln(os.Stderr, "Available games:")
	fmt.Fprintln(os.Stderr, "  #  id        name                  size")
	for i, k := range keys {
		g := m.Games[k]
		fmt.Fprintf(os.Stderr, "  %-3d %-9s %-21s %s\n", i+1, k, g.Name, humanBytes(totalSize(g)))
	}
}

func flagSet(name string) bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

func promptRegion() string {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Fprint(os.Stderr, "Region (NA or EU, default EU): ")
		line, err := reader.ReadString('\n')
		s := strings.TrimSpace(strings.ToLower(line))
		if err != nil && s == "" {
			return "eu"
		}
		switch s {
		case "":
			return "eu"
		case "na", "eu":
			return s
		default:
			fmt.Fprintf(os.Stderr, "invalid region %q, use NA or EU\n", s)
		}
	}
}

func prompt() string {
	fmt.Fprint(os.Stderr, "Which game do you want to download? ")
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && line == "" {
		return ""
	}
	return strings.TrimSpace(line)
}

func resolve(sel string, keys []string) (game, error) {
	if num, ok := atoi(sel); ok && num >= 1 && num <= len(keys) {
		return m.Games[keys[num-1]], nil
	}
	for _, k := range keys {
		g := m.Games[k]
		if strings.EqualFold(k, sel) || strings.EqualFold(g.Name, sel) {
			return g, nil
		}
		if strings.Contains(strings.ToLower(g.Name), strings.ToLower(sel)) {
			return g, nil
		}
	}
	return game{}, errors.New("no game matched " + fmt.Sprintf("%q", sel))
}

func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}

func totalSize(g game) int64 {
	var n int64
	for _, f := range g.Files {
		n += f.S
	}
	return n
}

const maxAttempts = 3

var stdin io.Reader = os.Stdin

type failure struct {
	idx    int
	path   string
	reason string
}

// download fetches every file in g that is not already present and valid.
func download(ctx context.Context, g game, base, cwd string, jobs int, keepGoing bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bar := newBar(len(g.Files), totalSize(g))
	go bar.refreshLoop(ctx)

	need, invalid, valid := verifyExisting(ctx, g, cwd, jobs, bar)
	bar.pauseRender()
	if valid > 0 {
		fmt.Fprintf(os.Stderr, "Verified %d file(s) already on disk.\n", valid)
	}

	var fails []failure
	skippedInvalid := len(invalid)
	if len(invalid) > 0 {
		fmt.Fprintf(os.Stderr, "\n%d of %d files on disk failed hash verification:\n", len(invalid), len(g.Files))
		for _, x := range invalid {
			fmt.Fprintf(os.Stderr, "  %s (%s)\n", x.path, x.reason)
		}
		if promptYesNo(fmt.Sprintf("Re-download %d file(s)?", len(invalid))) {
			for _, x := range invalid {
				need = append(need, x.idx)
			}
			skippedInvalid = 0
		} else {
			fails = append(fails, invalid...)
		}
	}
	if len(need) > 0 {
		bar.resumeRender()
	}

	type job struct {
		f      fileInfo
		target string
	}

	var (
		mu       sync.Mutex
		done     = int64(valid) + int64(skippedInvalid)
		abortErr error
	)
	jobsCh := make(chan job)
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobsCh {
				if ctx.Err() != nil {
					return
				}
				bar.begin()
				if err := downloadFile(ctx, j.f, g.Folder, base, j.target, bar); err != nil {
					bar.abort()
					if ctx.Err() != nil {
						return
					}
					mu.Lock()
					if !keepGoing {
						if abortErr == nil {
							abortErr = fmt.Errorf("%s: %v (stopped after %d of %d files)", j.f.P, err, done, len(g.Files))
						}
						mu.Unlock()
						cancel()
						return
					}
					fails = append(fails, failure{path: j.f.P, reason: err.Error()})
					done++
					mu.Unlock()
					continue
				}
				bar.commit()
				mu.Lock()
				done++
				mu.Unlock()
			}
		}()
	}

	aborted := false
	for _, i := range need {
		if aborted {
			break
		}
		f := g.Files[i]
		target := filepath.Join(cwd, filepath.FromSlash(f.P))
		select {
		case jobsCh <- job{f, target}:
		case <-ctx.Done():
			aborted = true
		}
	}
	close(jobsCh)
	wg.Wait()

	mu.Lock()
	abort := abortErr
	collected := append([]failure(nil), fails...)
	final := int(done) - len(collected)
	mu.Unlock()

	bar.stop(final)

	if abort != nil {
		return abort
	}
	if ctx.Err() != nil {
		return fmt.Errorf("interrupted after %d of %d files", done, len(g.Files))
	}
	if len(collected) > 0 {
		reportFailures(collected)
		return fmt.Errorf("%d of %d files failed (first: %s)", len(collected), len(g.Files), collected[0].path)
	}
	if done != int64(len(g.Files)) {
		return fmt.Errorf("interrupted after %d of %d files", done, len(g.Files))
	}
	return nil
}

// verifyExisting hashes every file already on disk. Absent or unreadable
// files are queued in need; corrupt ones are returned in invalid; valid ones
// are counted into bar. The hashing runs in parallel across jobs workers, so a
// full re-verify of thousands of files is no longer a single-threaded pause.
func verifyExisting(ctx context.Context, g game, cwd string, jobs int, bar *progressBar) (need []int, invalid []failure, valid int) {
	type item struct {
		idx  int
		f    fileInfo
		path string
	}
	var toVerify []item
	for i, f := range g.Files {
		path := filepath.Join(cwd, filepath.FromSlash(f.P))
		if _, err := os.Stat(path); err != nil {
			need = append(need, i)
			continue
		}
		toVerify = append(toVerify, item{idx: i, f: f, path: path})
	}
	if len(toVerify) == 0 {
		return need, nil, 0
	}

	workCh := make(chan item, len(toVerify))
	for _, it := range toVerify {
		workCh <- it
	}
	close(workCh)

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	workers := jobs
	if workers > len(toVerify) {
		workers = len(toVerify)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range workCh {
				if ctx.Err() != nil {
					return
				}
				ok, err := verifyFile(it.path, it.f.H)
				switch {
				case err != nil:
					mu.Lock()
					need = append(need, it.idx)
					mu.Unlock()
				case ok:
					bar.preskip(it.f.S, 1)
					mu.Lock()
					valid++
					mu.Unlock()
				default:
					mu.Lock()
					invalid = append(invalid, failure{idx: it.idx, path: it.f.P, reason: "hash mismatch"})
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return need, invalid, valid
}

func reportFailures(fails []failure) {
	sort.Slice(fails, func(i, j int) bool { return fails[i].path < fails[j].path })
	fmt.Fprintf(os.Stderr, "\n%d file(s) did not complete:\n", len(fails))
	for _, f := range fails {
		fmt.Fprintf(os.Stderr, "  %s: %s\n", f.path, f.reason)
	}
}

// promptYesNo asks a yes/no question from stdin, defaulting to no.
func promptYesNo(question string) bool {
	fmt.Fprintf(os.Stderr, "%s [y/N]: ", question)
	line, err := bufio.NewReader(stdin).ReadString('\n')
	if err != nil && line == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	}
	return false
}

// checkAvailability does a 1-byte Range probe so an unreachable CDN fails before the pool starts.
func checkAvailability(ctx context.Context, g game, base string) error {
	if len(g.Files) == 0 {
		return nil
	}
	f := g.Files[0]
	url := base + g.Folder + "/" + encodePath(f.P) + "?" + f.H

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "cb-downloader/1.0")
	req.Header.Set("Range", "bytes=0-0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %v", f.P, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("%s: HTTP %d", f.P, resp.StatusCode)
	}
	return nil
}

type hashWriter struct {
	hasher *xxh3.Hasher
	bar    *progressBar
	n      int64
}

func (w *hashWriter) Write(p []byte) (int, error) {
	w.hasher.Write(p)
	w.bar.add(int64(len(p)))
	w.n += int64(len(p))
	return len(p), nil
}

// rollback subtracts this attempt's bytes from the progress bar.
func (w *hashWriter) rollback() {
	w.bar.add(-w.n)
	w.n = 0
}

func (w *hashWriter) sum() string { return formatHash(w.hasher.Sum64()) }

// downloadFile fetches and hash-verifies one file, retrying 5xx and transport errors.
func downloadFile(ctx context.Context, f fileInfo, folder, base, target string, bar *progressBar) error {
	url := base + folder + "/" + encodePath(f.P) + "?" + f.H

	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-time.After(time.Duration(attempt-1) * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		last = fetchOnce(ctx, f, url, target, bar)
		if last == nil {
			return nil
		}
		if ctx.Err() != nil {
			return last
		}
		if !retryable(last) {
			return fmt.Errorf("%s: %w", f.P, last)
		}
	}
	return fmt.Errorf("%s: %w (after %d attempts)", f.P, last, maxAttempts)
}

// retryable reports whether an error is worth retrying — 5xx and transport errors are.
func retryable(err error) bool {
	var se statusError
	if errors.As(err, &se) {
		return se.code >= 500
	}
	if errors.Is(err, errHashMismatch) {
		return false
	}
	var pe *os.PathError
	if errors.As(err, &pe) {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

var errHashMismatch = errors.New("hash mismatch")

type statusError struct{ code int }

func (e statusError) Error() string { return fmt.Sprintf("HTTP %d", e.code) }

// fetchOnce performs one download attempt; a hash mismatch leaves the file for inspection.
func fetchOnce(ctx context.Context, f fileInfo, url, target string, bar *progressBar) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "cb-downloader/1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError{resp.StatusCode}
	}

	if dir := filepath.Dir(target); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	out, err := os.Create(target)
	if err != nil {
		return err
	}
	defer out.Close()

	w := &hashWriter{hasher: xxh3.New(), bar: bar}
	if _, err := io.Copy(out, io.TeeReader(resp.Body, w)); err != nil {
		w.rollback()
		out.Close()
		os.Remove(target)
		return err
	}
	if err := out.Close(); err != nil {
		w.rollback()
		os.Remove(target)
		return err
	}

	if got := w.sum(); got != f.H {
		return fmt.Errorf("%w: got %s want %s", errHashMismatch, got, f.H)
	}
	return nil
}

func encodePath(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		if isSafe(c) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func isSafe(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == '.' || c == '~' || c == '/'
}

// formatHash renders an XXH3-64 digest as uppercase hex of its little-endian bytes (NOT %016X); see TestHashGolden.
func formatHash(v uint64) string {
	sum := make([]byte, 8)
	binary.LittleEndian.PutUint64(sum, v)
	return strings.ToUpper(hex.EncodeToString(sum))
}

func hashBytes(b []byte) string { return formatHash(xxh3.Hash(b)) }

// hashFile streams path through XXH3-64. Used to verify files already on disk.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := xxh3.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return formatHash(h.Sum64()), nil
}

// verifyFile reports whether path hashes to want; on read error ok is false and err is set.
func verifyFile(path, want string) (bool, error) {
	got, err := hashFile(path)
	if err != nil {
		return false, err
	}
	return got == want, nil
}

type progressBar struct {
	mu sync.Mutex

	total int64
	done  int64

	active     int
	filesDone  int
	filesTotal int

	lastRender time.Time
	lastBytes  int64
	lastLine   int

	paused bool
}

func newBar(files int, size int64) *progressBar {
	return &progressBar{
		filesTotal: files,
		total:      size,
	}
}

func (b *progressBar) refreshLoop(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.render()
		}
	}
}

func (b *progressBar) render() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.paused {
		return
	}

	now := time.Now()
	elapsed := now.Sub(b.lastRender).Seconds()
	var speed float64
	if !b.lastRender.IsZero() && elapsed > 0 {
		speed = float64(b.done-b.lastBytes) / elapsed
	}
	b.lastBytes = b.done
	b.lastRender = now

	var overall float64
	if b.total > 0 {
		overall = float64(b.done) / float64(b.total) * 100
	}
	line := fmt.Sprintf("\r[%d/%d files, %d active]  overall %5.1f%%  (%s / %s)  |  %s/s",
		b.filesDone, b.filesTotal, b.active, overall,
		humanBytes(b.done), humanBytes(b.total), humanBytes(int64(speed)))

	if len(line) < b.lastLine {
		line += strings.Repeat(" ", b.lastLine-len(line))
	}
	b.lastLine = len(line)
	fmt.Fprint(os.Stderr, line)
}

func (b *progressBar) add(n int64) {
	b.mu.Lock()
	b.done += n
	b.mu.Unlock()
}

// preskip seeds the bar with already-verified files so resume starts on the true position.
func (b *progressBar) preskip(bytes int64, files int) {
	b.mu.Lock()
	b.done += bytes
	b.filesDone += files
	b.mu.Unlock()
}

func (b *progressBar) begin() {
	b.mu.Lock()
	b.active++
	b.mu.Unlock()
}

func (b *progressBar) abort() {
	b.mu.Lock()
	b.active--
	b.mu.Unlock()
}

func (b *progressBar) commit() {
	b.mu.Lock()
	b.active--
	b.filesDone++
	b.mu.Unlock()
}

// pauseRender stops live redraws and breaks the line so stderr text (the verify
// summary, invalid-file list, and prompt) is not clobbered by the 250 ms render.
func (b *progressBar) pauseRender() {
	b.mu.Lock()
	b.paused = true
	b.mu.Unlock()
	fmt.Fprintln(os.Stderr)
}

// resumeRender re-enables redraws and paints the current state once.
func (b *progressBar) resumeRender() {
	b.mu.Lock()
	b.paused = false
	b.mu.Unlock()
	b.render()
}

// stop ends the refresh loop and finalizes the line with the real success count.
// It forces a render even from a paused state, since this is the terminal paint.
func (b *progressBar) stop(filesDone int) {
	b.mu.Lock()
	b.active = 0
	b.filesDone = filesDone
	b.paused = false
	b.mu.Unlock()
	b.render()
	fmt.Fprintln(os.Stderr)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	pause()
	os.Exit(1)
}

var interactive bool

// pause waits for Enter before exit, only in interactive (no -game) mode.
func pause() {
	if !interactive {
		return
	}
	fmt.Fprint(os.Stderr, "\nPress Enter to close...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}
