package etcdmgmt

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gluster/glusterd2/context"
	"github.com/gluster/glusterd2/utils"

	log "github.com/Sirupsen/logrus"
)

// ExecName is the etcd executable name, useful for mocking in tests
var (
	listenClientUrls   string
	advClientUrls      string
	listenPeerUrls     string
	initialAdvPeerUrls string
	ExecName           = "etcd"
	etcdPidDir         = "/var/run/gluster/"
	etcdPidFile        = etcdPidDir + "etcd.pid"
	etcdConfDir        = "/var/lib/glusterd/"
	etcdConfFile       = etcdConfDir + "etcdenv.conf"
)

// checkETCDHealth ensures that etcd has come up properly
func checkETCDHealth(waitTime time.Duration, listenClientUrls string) bool {
	result := struct{ Health string }{}

	// Checking health of etcd. Health of the etcd should be true,
	// means etcd have initialized properly before using any etcd command
	timer := time.NewTimer(time.Second * waitTime)
	for {
		// Waiting for 15 second. Within 15 second health of etcd should
		// be true otherwise it should throw an error
		go func() {
			<-timer.C
			if result.Health != "true" {
				log.Fatal("Health of etcd is not proper. Check etcd configuration.")
			}
		}()

		resp, err := http.Get(listenClientUrls + "/health")
		if err != nil {
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)

		err = json.Unmarshal([]byte(body), &result)
		if err != nil {
			continue
		}
		if result.Health == "true" {
			timer.Stop()
			break
		}
	}
	return true
}

// StartETCD brings up an etcd instance
func StartETCD(args []string) (*os.Process, error) {
	start, pid := isETCDStartNeeded()
	if start == false {
		log.WithField("pid", pid).Info("etcd instance is already running")
		etcdCtx, e := os.FindProcess(pid)
		return etcdCtx, e
	}

	log.WithField("Executable", ExecName).Info("Starting")

	etcdCmd := exec.Command(ExecName, args...)

	// TODO: use unix.Setpgid instead of using syscall
	// Don't kill chlid process (etcd) upon ^C (SIGINT) of main glusterd process
	etcdCmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	err := etcdCmd.Start()
	if err != nil {
		log.WithField("error", err.Error()).Error("Could not start etcd daemon.")
		return nil, err
	}

	// Wait for etcd to come up
	if check := checkETCDHealth(15, args[1]); check != true {
		log.Fatal("Health of etcd is not proper. Check etcd configuration.")
	}

	log.WithField("pid", etcdCmd.Process.Pid).Debug("etcd started")

	if err := writeETCDPidFile(etcdCmd.Process.Pid); err != nil {
		etcdCmd.Process.Kill()
		return nil, err
	}

	return etcdCmd.Process, nil
}

// writeETCDPidFile () is to write the pid of etcd instance
func writeETCDPidFile(pid int) error {
	// create directory to store etcd pid if it doesn't exist
	utils.InitDir(etcdPidDir)
	if err := ioutil.WriteFile(etcdPidFile, []byte(strconv.Itoa(pid)), os.ModePerm); err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"path":  etcdPidFile,
			"pid":   string(pid),
		}).Error("Failed to write etcd pid to the file")
		return err
	}
	return nil
}

// isETCDStartNeeded decides if etcd needs to be started by reading the etcd pidfile.
// If a pid is found, checks if the process is running.
// If the process is running, checks if etcd health is good.
// Returns false and the pid if health is good.
// Returns true and -1 in any other case.
func isETCDStartNeeded() (bool, int) {
	pid := -1
	start := true
	bytes, err := ioutil.ReadFile(etcdPidFile)
	if err == nil {
		pidString := string(bytes)
		if pid, err = strconv.Atoi(pidString); err != nil {
			log.WithField("pid", pidString).Error("Failed to convert string to integer")
			start = true
			return start, pid
		}
		if exist := utils.CheckProcessExist(pid); exist == true {
			hostname, err := os.Hostname()
			if err != nil {
				log.Fatal("Could not get hostname")
			}
			listenClientUrls := "http://" + hostname + ":2379"
			_, err = http.Get(listenClientUrls + "/health")
			if err != nil {
				log.WithField("err", err).Error("etcd health check failed")
				pid = -1
				start = true
			} else {
				start = false
			}
		}
	} else {
		switch {
		case os.IsNotExist(err):
			start = true
			break
		default:
			log.WithFields(log.Fields{
				"error": err,
				"path":  etcdPidFile,
			}).Fatal("Failed to read from file")
		}
	}
	return start, pid
}

// initETCDArgVar() will initialize etcd argument which will be used at various places
func initETCDArgVar() {
	context.SetLocalHostIP()

	listenClientUrls = "http://" + context.HostIP + ":2379"
	advClientUrls = "http://" + context.HostIP + ":2379"
	listenPeerUrls = "http://" + context.HostIP + ":2380"
	initialAdvPeerUrls = "http://" + context.HostIP + ":2380"
}

// ETCDStartInit checks whether etcd environment variable is present.
// If present, etcd is started without `--initial-cluster` flag.
// Else etcd is started normally.
func ETCDStartInit() (*os.Process, error) {
	initETCDArgVar()

	file, err := os.Open(etcdConfFile)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			log.Info("Starting/Restarting etcd for a initial node")
			return StartStandAloneETCD()
		default:
			log.WithFields(log.Fields{
				"error": err,
				"path":  etcdPidFile,
			}).Fatal("Failed to read from file")
		}
	} else {
		defer file.Close()

		// Restoring etcd environment variable and starting etcd daemon
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			linestr := scanner.Text()
			etcdenv := strings.Split(linestr, "=")

			etcdEnvKey := etcdenv[0]
			etcdEnvData := etcdenv[1]
			envlen := len(etcdenv)
			for i := 2; i < envlen; i++ {
				etcdEnvData = etcdEnvData + "=" + etcdenv[i]
			}

			// setting etcd environment variable
			os.Setenv(etcdEnvKey, etcdEnvData)
		}

		args := []string{"-listen-client-urls", listenClientUrls,
			"-advertise-client-urls", advClientUrls,
			"-listen-peer-urls", listenPeerUrls,
			"-initial-advertise-peer-urls", initialAdvPeerUrls}

		log.Info("Sstarting etcd daemon")
		return StartETCD(args)
	}
	return nil, err
}

// StartStandAloneETCD starts etcd in single server mode
func StartStandAloneETCD() (*os.Process, error) {
	args := []string{"-listen-client-urls", listenClientUrls,
		"-advertise-client-urls", advClientUrls,
		"-listen-peer-urls", listenPeerUrls,
		"-initial-advertise-peer-urls", initialAdvPeerUrls,
		"--initial-cluster", "default=" + listenPeerUrls}

	return StartETCD(args)
}

// StopETCD stops etcd process
func StopETCD(etcdCtx *os.Process) error {
	err := etcdCtx.Kill()
	if err != nil {
		log.WithError(err).Error("Could not kill etcd daemon")
		return err
	}
	_, err = etcdCtx.Wait()
	if err != nil {
		log.WithError(err).Error("Could not kill etcd daemon")
		return err
	}
	return nil
}

// ReStartETCD restarts etcd
func ReStartETCD() (*os.Process, error) {
	// Stop etcd process
	etcdCtx := context.EtcdProcessCtx
	err := StopETCD(etcdCtx)
	if err != nil {
		log.WithError(err).Error("Could not stop etcd daemon")
		return nil, err
	}

	args := []string{"-listen-client-urls", listenClientUrls,
		"-advertise-client-urls", advClientUrls,
		"-listen-peer-urls", listenPeerUrls,
		"-initial-advertise-peer-urls", initialAdvPeerUrls}

	log.Info("Restarting etcd daemon")

	return StartETCD(args)
}
