package seqkit

import (
	"fmt"
	"ignis/driver/api"
	"regexp"
	"strconv"
	"strings"
)

type SeqKitRangeOptions struct {
	inner RangeOptions
}

type RangeOptions struct {
	Config  KitConfig
	Range   *string
	TwoPass *bool
}

func (this *RangeOptions) setDefaults() *RangeOptions {
	this.Config.setDefaults()
	setDefault(&this.Range, "")

	return this
}

func (this *SeqKitRangeOptions) Config(v *SeqKitConfig) *SeqKitRangeOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitRangeOptions) Range(v string) *SeqKitRangeOptions {
	this.inner.Range = &v
	return this
}

func (this *SeqKitRangeOptions) TwoPass(v bool) *SeqKitRangeOptions {
	this.inner.TwoPass = &v
	return this
}

func Range(input *api.IDataFrame[string], o *SeqKitRangeOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitRangeOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	if *opts.Range == "" {
		return nil, fmt.Errorf("flag -r (--range) needed")
	}
	var reRegion = regexp.MustCompile(`\-?\d+:\-?\d+`)
	if !reRegion.MatchString(*opts.Range) {
		return nil, fmt.Errorf(`invalid range: %s. type "seqkit range -h" for more examples`, *opts.Range)
	}

	r := strings.Split(*opts.Range, ":")
	start, err := strconv.ParseInt(r[0], 10, 64)
	if err != nil {
		return nil, err
	}
	end := int64(-1)
	if len(r) > 1 {
		end, err = strconv.ParseInt(r[1], 10, 64)
		if err != nil {
			return nil, err
		}
	}

	if start == 0 || end == 0 {
		return nil, fmt.Errorf("either start and end should not be 0")
	}

	if start > 0 {
		start--
	}
	if end == -1 {
		end = 1<<63 - 1
	}

	if start < -1 || end < -1 {
		if !*opts.TwoPass {
			if err := input.Cache(api.PRESERVE); err != nil {
				return nil, err
			}
		}

		n, err := input.Count()
		if err != nil {
			return nil, err
		}
		if start < 0 {
			start += n
		}
		if end < 0 {
			end += n
		}
	}

	if start <= end {
		return nil, fmt.Errorf(" start must be > than end")
	}

	libprepare := libSource("RangePrepare")
	libprepare, err = api.AddParam[int64](libprepare, "start", start)
	if err != nil {
		return nil, err
	}
	libprepare, err = api.AddParam[int64](libprepare, "end", end)
	if err != nil {
		return nil, err
	}
	prepared, err := api.MapWithIndex[string, string](input, libprepare)
	if err != nil {
		return nil, err
	}

	return prepared.Filter(libSource("RangeFilter"))
}
