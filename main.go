package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
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
		region = flag.String("region", "eu", "CDN region: na or eu")
		want   = flag.String("game", "", "game id, name or number; skips the interactive prompt")
		jobs   = flag.Int("jobs", 8, "max files downloaded simultaneously")
	)
	flag.Parse()

	if err := json.Unmarshal(embeddedData, &m); err != nil {
		fail("failed to parse embedded data: %v", err)
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

	keys := gameKeys()
	sel := strings.TrimSpace(*want)
	if sel == "" {
		printMenu(keys)
		sel = prompt()
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

	if err := download(g, base, cwd, *jobs); err != nil {
		fail("%v", err)
	}
	fmt.Fprintf(os.Stderr, "Finished! Game files are in %s\n", cwd)
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

func download(g game, base, cwd string, jobs int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bar := newBar(len(g.Files), totalSize(g))
	go bar.refreshLoop(ctx)

	var (
		mu     sync.Mutex
		done   int64
		failed int64
	)
	sem := make(chan struct{}, jobs)
	var wg sync.WaitGroup

	for _, f := range g.Files {
		target := filepath.Join(cwd, filepath.FromSlash(f.P))
		if fi, err := os.Stat(target); err == nil && fi.Size() == f.S {
			bar.skip(f.S)
			mu.Lock()
			done++
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(f fileInfo, target string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			bar.begin()
			if err := downloadFile(f, g.Folder, base, target, bar); err != nil {
				bar.warn(fmt.Sprintf("%s: %v", f.P, err))
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			bar.commit()
			mu.Lock()
			done++
			mu.Unlock()
		}(f, target)
	}
	wg.Wait()

	bar.stop()

	if failed > 0 {
		return fmt.Errorf("%d of %d files failed to download", failed, len(g.Files))
	}
	if done != int64(len(g.Files)) {
		return fmt.Errorf("interrupted after %d of %d files", done, len(g.Files))
	}
	return nil
}

func downloadFile(f fileInfo, folder, base, target string, bar *progressBar) error {
	url := base + folder + "/" + encodePath(f.P) + "?" + f.H

	req, err := http.NewRequest(http.MethodGet, url, nil)
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
		return fmt.Errorf("HTTP %d", resp.StatusCode)
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

	writer := &progressWriter{bar}
	if _, err := io.Copy(out, io.TeeReader(resp.Body, writer)); err != nil {
		os.Remove(target)
		return err
	}
	return nil
}

type progressWriter struct {
	bar *progressBar
}

func (w *progressWriter) Write(p []byte) (int, error) {
	w.bar.add(int64(len(p)))
	return len(p), nil
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

func (b *progressBar) skip(size int64) {
	b.mu.Lock()
	b.done += size
	b.mu.Unlock()
}

func (b *progressBar) begin() {
	b.mu.Lock()
	b.active++
	b.mu.Unlock()
}

func (b *progressBar) commit() {
	b.mu.Lock()
	b.active--
	b.filesDone++
	b.mu.Unlock()
}

func (b *progressBar) warn(msg string) {
	b.mu.Lock()
	fmt.Fprintf(os.Stderr, "\nWARNING: %s\n", msg)
	b.mu.Unlock()
}

func (b *progressBar) stop() {
	b.mu.Lock()
	b.active = 0
	b.filesDone = b.filesTotal
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
	os.Exit(1)
}
