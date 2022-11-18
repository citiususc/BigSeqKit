package main

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
	"io/ioutil"
	"os/exec"
)

type cmdPipe struct {
	Pipe []*cmdPipe `json:"pipe"`
	Cmd  []string   `json:"cmd"`
	sh   *string    `json:"sh"`
}

func runCmd(c *cmdPipe) {
	jobInput = make([]*api.IDataFrame[string], 0)
	jobOuput = nil
	fOuput = nil
	result := make([]*api.IDataFrame[string], 0)
	if c.Pipe != nil {
		for _, dep := range c.Pipe {
			runCmd(dep)
			if jobOuput == nil {
				checkError(fmt.Errorf("bad execution dependency"))
			}
			result = append(result, jobOuput)
		}
		jobInput = result
	}

	if c.sh != nil && *c.sh != " " {
		check(exec.Command("sh", "-c", *c.sh).Output())
	}

	parser := Parser()
	parser.SetArgs(c.Cmd)
	checkError(parser.Execute())
}

func runPipe(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	job := getFlagString(cmd, "job")
	if job == "" {
		checkError(fmt.Errorf("job not defined"))
	}
	p := cmdPipe{}
	if err := json.Unmarshal(check(ioutil.ReadFile(job)), &p); err != nil {
		checkError(fmt.Errorf("incorrect job format"))
	}

	runCmd(&p)
	if jobOuput == nil {
		if len(input) > 0 {
			return union(cmd, input...)
		} else {
			return nil
		}
	}

	result := make([]*api.IDataFrame[string], 0, len(input)+1)
	result = append(result, jobOuput)
	result = append(result, input...)

	return union(cmd, result...)
}

func init() {
	addCommand(func(parent *cobra.Command) {
		cmd := &cobra.Command{
			Use:   "pipe",
			Short: "execute multiple commands like a pipe in the same job",
			Long:  `execute multiple commands like a pipe in the same job`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runPipe)
			},
		}

		parent.AddCommand(cmd)

		cmd.Flags().String("job", "", "job definition")
	})
}
