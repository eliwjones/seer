package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

func main() {
	args := [][]string{}
	for i := 0; i < 100; i++ {
		updport := fmt.Sprintf(`%d`, 9000+i)
		tcpport := fmt.Sprintf(`%d`, 10000+i)
		ipaddress := `127.0.0.1`
		argarray := []string{
			`-ip=` + ipaddress,
			`-tcp=` + tcpport,
			`-udp=` + updport}

		if i > 0 {
			argarray = append(argarray, `-seeds=127.0.0.1:9000`)
		}
		args = append(args, argarray)
	}
	fmt.Printf("%v\n", args[0])
	_, _ = runInBackground(`seer`, args[0], true)
	for idx, arg := range args {
		if idx == 0 {
			continue
		}
		fmt.Printf("idx:      %d\nargs: %v\n", idx, arg)
		_, err := runInBackground(`seer`, arg, false)
		if err != nil {
			fmt.Println("Err: " + err.Error())
		}
	}
}

func runInBackground(cmdname string, cmdargs []string, redirectoutput bool) (*exec.Cmd, error) {
	cmdargs = append([]string{cmdname}, cmdargs...)
	cmd := &exec.Cmd{
		Path: cmdname,
		Args: cmdargs,
	}
	if redirectoutput {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	err := cmd.Start()
	time.Sleep(1 * time.Second)
	return cmd, err
}
