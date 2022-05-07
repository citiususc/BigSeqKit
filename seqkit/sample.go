package seqkit

import (
	"fmt"
	"ignis/driver/api"
)

type SeqKitSampleOptions struct {
	inner SampleOptions
}

type SampleOptions struct {
	Config     KitConfig
	Seed       *int
	Number     *int
	Proportion *float32
	TwoPass    *bool
}

func (this *SampleOptions) setDefaults() *SampleOptions {
	this.Config.setDefaults()
	setDefault(&this.Seed, 11)
	setDefault(&this.Number, 0)
	setDefault(&this.Proportion, 0)
	setDefault(&this.TwoPass, false)

	return this
}

func (this *SeqKitSampleOptions) Config(v *SeqKitConfig) *SeqKitSampleOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitSampleOptions) Seed(v int) *SeqKitSampleOptions {
	this.inner.Seed = &v
	return this
}

func (this *SeqKitSampleOptions) Number(v int) *SeqKitSampleOptions {
	this.inner.Number = &v
	return this
}

func (this *SeqKitSampleOptions) Proportion(v float32) *SeqKitSampleOptions {
	this.inner.Proportion = &v
	return this
}

func (this *SeqKitSampleOptions) TwoPass(v bool) *SeqKitSampleOptions {
	this.inner.TwoPass = &v
	return this
}

func Sample(input *api.IDataFrame[string], o *SeqKitSampleOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitSampleOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	if *opts.Number == 0 && *opts.Proportion == 0 {
		return nil, fmt.Errorf("one of flags -n (--number) and -p (--proportion) needed")
	}

	if *opts.Number < 0 {
		return nil, fmt.Errorf("value of -n (--number) and should be greater than 0")
	}
	if *opts.Proportion < 0 || *opts.Proportion > 1 {
		return nil, fmt.Errorf("value of -p (--proportion) (%f) should be in range of (0, 1]", *opts.Proportion)
	}

	fraction := float64(*opts.Proportion)
	if *opts.Number > 0 {
		if !*opts.TwoPass {
			if err := input.Cache(api.PRESERVE); err != nil {
				return nil, err
			}
		}
		n, err := input.Count()
		if err != nil {
			return nil, err
		}
		fraction = float64(*opts.Number) / float64(n)
	}

	return input.Sample(false, fraction, *opts.Seed)
}
