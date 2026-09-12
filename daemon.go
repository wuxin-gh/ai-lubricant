package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

const (
	daemonChildEnv    = "_ATOMCODE_DAEMON_CHILD"
	daemonSupervisorEnv = "_ATOMCODE_DAEMON_SUPERVISOR"
	daemonPortEnv     = "_ATOMCODE_DAEMON_PORT"
	daemonVerboseEnv  = "_ATOMCODE_DAEMON_VERBOSE"
	pidFileName       = ".atomcode-2api/daemon.pid"
	maxRestartDelay   = 30 * time.Second
	baseRestartDelay  = 1 * time.Second
)

var daemonPIDFile string

var daemonCmd = &cobra.Command{
	Use:     "daemon",
	Short:   "以守护进程模式运行（崩溃自动重启）",
	Long:    "以后台守护进程模式启动代理服务。自动在后台运行，崩溃后自动重启（指数退避），日志写入文件。",
	GroupID: "service",
}

var daemonStartCmd = &cobra.Command{
	Use:   "start",
	Short: "启动守护进程",
	RunE: func(cmd *cobra.Command, args []string) error {
		return startDaemon()
	},
}

var daemonStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "停止守护进程",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stopDaemon()
	},
}

var daemonRestartCmd = &cobra.Command{
	Use:   "restart",
	Short: "重启守护进程",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := stopDaemon(); err != nil {
			log.Printf("stop warning: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
		return startDaemon()
	},
}

var daemonStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "查看守护进程状态",
	RunE: func(cmd *cobra.Command, args []string) error {
		return daemonStatus()
	},
}

func init() {
	home, _ := os.UserHomeDir()
	daemonPIDFile = filepath.Join(home, pidFileName)

	daemonCmd.AddCommand(daemonStartCmd)
	daemonCmd.AddCommand(daemonStopCmd)
	daemonCmd.AddCommand(daemonRestartCmd)
	daemonCmd.AddCommand(daemonStatusCmd)
	daemonCmd.PersistentFlags().IntVarP(&servePort, "port", "p", 45678, "绑定端口")
	rootCmd.AddCommand(daemonCmd)
}

func startDaemon() error {
	if os.Getenv(daemonChildEnv) != "" || os.Getenv(daemonSupervisorEnv) == "1" {
		return fmt.Errorf("already running as daemon")
	}

	if pid, running := checkRunningDaemon(); running {
		return fmt.Errorf("daemon already running (PID %d)", pid)
	}

	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot determine binary path: %w", err)
	}

	env := append(os.Environ(),
		daemonSupervisorEnv+"=1",
		daemonPortEnv+"="+strconv.Itoa(servePort),
	)
	if verbose {
		env = append(env, daemonVerboseEnv+"=1")
	}

	cmd := exec.Command(binPath)
	cmd.Env = env
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = procAttr()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start daemon supervisor: %w", err)
	}
	cmd.Process.Release()
	fmt.Printf("Daemon started (PID %d, port %d)\n", cmd.Process.Pid, servePort)
	return nil
}

func stopDaemon() error {
	pidData, err := readPIDFile()
	if err != nil {
		fmt.Println("Daemon not running.")
		return nil
	}

	proc, err := os.FindProcess(pidData.PID)
	if err != nil {
		removePIDFile()
		fmt.Println("Daemon not running (process not found).")
		return nil
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		removePIDFile()
		return nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := proc.Wait()
		done <- err
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		proc.Signal(syscall.SIGKILL)
	}

	removePIDFile()
	fmt.Printf("Daemon stopped (was PID %d)\n", pidData.PID)
	return nil
}

func daemonStatus() error {
	pidData, err := readPIDFile()
	if err != nil {
		fmt.Println("Daemon not running.")
		return nil
	}

	proc, err := os.FindProcess(pidData.PID)
	if err != nil {
		fmt.Printf("Daemon PID %d — process lookup failed\n", pidData.PID)
		return nil
	}

	if err := proc.Signal(syscall.Signal(0)); err != nil {
		fmt.Printf("Daemon NOT running (stale PID file)\n")
		removePIDFile()
		return nil
	}

	fmt.Printf("Daemon running (PID %d, port %d)\n", pidData.PID, pidData.Port)
	return nil
}

// ─── Supervisor Loop ────────────────────────────────────────────────────────────

type daemonPID struct {
	PID       int    `json:"pid"`
	Port      int    `json:"port"`
	StartedAt string `json:"started_at"`
}

func RunSupervisor(port int) {
	log.Printf("[supervisor] starting (PID %d, port %d)", os.Getpid(), port)

	writePIDFile(daemonPID{
		PID:       os.Getpid(),
		Port:      port,
		StartedAt: time.Now().Format(time.RFC3339),
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var mu sync.Mutex
	delay := baseRestartDelay

	for {
		binPath, err := os.Executable()
		if err != nil {
			log.Fatalf("[supervisor] cannot find binary: %v", err)
		}

		args := []string{"serve", "--port", strconv.Itoa(port)}
		if os.Getenv(daemonVerboseEnv) == "1" {
			args = append(args, "-v")
		}
		if skipValidation {
			args = append(args, "--skip-validation")
		}

		cmd := exec.Command(binPath, args...)
		cmd.Env = append(os.Environ(), daemonChildEnv+"=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.SysProcAttr = procAttr()

		log.Printf("[supervisor] spawning child process")
		if err := cmd.Start(); err != nil {
			log.Printf("[supervisor] failed to start child: %v", err)
			mu.Lock()
			time.Sleep(delay)
			delay = minDuration(delay*2, maxRestartDelay)
			mu.Unlock()
			continue
		}

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				log.Printf("[supervisor] child crashed: %v — restarting", err)
			} else {
				log.Printf("[supervisor] child exited — restarting")
			}
			mu.Lock()
			time.Sleep(delay)
			delay = minDuration(delay*2, maxRestartDelay)
			mu.Unlock()

		case sig := <-sigCh:
			log.Printf("[supervisor] received %v — shutting down", sig)
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
			removePIDFile()
			log.Printf("[supervisor] stopped")
			return
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// ─── PID file management ─────────────────────────────────────────────────────

func writePIDFile(data daemonPID) error {
	if err := os.MkdirAll(filepath.Dir(daemonPIDFile), 0755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(data, "", "  ")
	return os.WriteFile(daemonPIDFile, b, 0644)
}

func readPIDFile() (daemonPID, error) {
	var data daemonPID
	b, err := os.ReadFile(daemonPIDFile)
	if err != nil {
		return data, err
	}
	err = json.Unmarshal(b, &data)
	return data, err
}

func removePIDFile() {
	os.Remove(daemonPIDFile)
}

func checkRunningDaemon() (int, bool) {
	data, err := readPIDFile()
	if err != nil {
		return 0, false
	}
	proc, err := os.FindProcess(data.PID)
	if err != nil {
		return 0, false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		removePIDFile()
		return 0, false
	}
	return data.PID, true
}
