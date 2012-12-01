package iptablesrun

import (
	"bytes"
	"io"
	"io/ioutil"
	"os/exec"
	"strings"
	"testing"
)

const (
	RULETP = "./iptables-tpl"
)

var originalRule string

func bakupIptables() error {
	saveCmd := exec.Command(SAVECMD, ARGS)
	if out, err := saveCmd.Output(); err != nil {
		return err
	} else {
		originalRule = string(out)
	}
	return nil
}

func restoreIptables(rules string) (err error) {
	if len(rules) <= 0 {
		return
	}
	restoreCmd := exec.Command(RESTORECMD, ARGS)
	var stdin io.WriteCloser
	if stdin, err = restoreCmd.StdinPipe(); err != nil {
		return
	}
	if err = restoreCmd.Start(); err != nil {
		return
	}
	var buf bytes.Buffer
	buf.WriteString(rules)
	buf.WriteTo(stdin)
	stdin.Close()
	if err = restoreCmd.Wait(); err != nil {
		return
	}
	return
}

func restoreTp(filename string, status int) error {
	if rules, err := ioutil.ReadFile(filename); err != nil {
		return err
	} else {
		var rs string
		if status == ACCEPT {
			rs = strings.Replace(string(rules), "TP_STATUS", "ACCEPT", -1)
		} else {
			rs = strings.Replace(string(rules), "TP_STATUS", "DROP", -1)
		}
		return restoreIptables(rs)
	}
	return nil
}

func TestFetchAll(t *testing.T) {
	if err := bakupIptables(); err != nil {
		t.Fatalf("Backup iptables error [%s]\n", err)
	}
	if err := restoreTp(RULETP, ACCEPT); err != nil {
		t.Fatalf("Restore iptables template error [%s]\n", err)
	}
	rules, err := FetchAll()
	if err != nil {
		t.Fatalf("Fetch all rules error [%s]\n", err)
	}
	for id, rule := range rules {
		if rule.Status == DROP {
			t.Errorf("Id [%s] status should not be DROP", id)
		}
	}
	restoreIptables(originalRule)
	if !t.Failed() {
		t.Log("FetchAll Passed")
	}
	return
}

func TestBlock(t *testing.T) {
	if err := bakupIptables(); err != nil {
		t.Fatalf("Backup iptables error [%s]\n", err)
	}
	if err := restoreTp(RULETP, ACCEPT); err != nil {
		t.Fatalf("Restore iptables template error [%s]\n", err)
	}
	rules, err := FetchAll()
	if err != nil {
		t.Fatalf("Fetch all rules error [%s]\n", err)
	}
	for id, _ := range rules {
		Block(id)
	}
	if err := Update(); err != nil {
		t.Fatalf("Update rules error [%s]\n", err)
	}
	rules, err = FetchAll()
	if err != nil {
		t.Fatalf("Fetch all rules again error [%s]\n", err)
	}
	for id, rule := range rules {
		if rule.Status == ACCEPT {
			t.Errorf("Container [%s] is not blocked", id)
		}
	}
	restoreIptables(originalRule)
	if !t.Failed() {
		t.Log("Block Passed")
	}
	return
}

func TestUnblock(t *testing.T) {
	if err := bakupIptables(); err != nil {
		t.Fatalf("Backup iptables error [%s]\n", err)
	}
	if err := restoreTp(RULETP, DROP); err != nil {
		t.Fatalf("Restore iptables template error [%s]\n", err)
	}
	rules, err := FetchAll()
	if err != nil {
		t.Fatalf("Fetch all rules error [%s]\n", err)
	}
	for id, _ := range rules {
		Unblock(id)
	}
	if err := Update(); err != nil {
		t.Fatalf("Update rules error [%s]\n", err)
	}
	rules, err = FetchAll()
	if err != nil {
		t.Fatalf("Fetch all rules again error [%s]\n", err)
	}
	for id, rule := range rules {
		if rule.Status == DROP {
			t.Errorf("Container [%s] is not unblocked", id)
		}
	}
	restoreIptables(originalRule)
	if !t.Failed() {
		t.Log("UnBlock Passed")
	}
	return
}
