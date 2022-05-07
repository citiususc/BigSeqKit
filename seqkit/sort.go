package seqkit

import (
	"fmt"
	"ignis/driver/api"
	"ignis/executor/api/ipair"
)

type SeqKitSortOptions struct {
	inner SortOptions
}

type SortOptions struct {
	Config          KitConfig
	InNaturalOrder  *bool
	BySeq           *bool
	ByName          *bool
	ByLength        *bool
	ByBases         *bool
	GapLetters      *string
	Reverse         *bool
	IgnoreCase      *bool
	SeqPrefixLength *uint
}

func (this *SortOptions) setDefaults() *SortOptions {
	this.Config.setDefaults()
	setDefault(&this.InNaturalOrder, false)
	setDefault(&this.BySeq, false)
	setDefault(&this.ByName, false)
	setDefault(&this.ByLength, false)
	setDefault(&this.ByBases, false)
	setDefault(&this.GapLetters, "- 	.")
	setDefault(&this.Reverse, false)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.SeqPrefixLength, 10000)

	return this
}

func (this *SeqKitSortOptions) Config(v *SeqKitConfig) *SeqKitSortOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitSortOptions) InNaturalOrder(v bool) *SeqKitSortOptions {
	this.inner.InNaturalOrder = &v
	return this
}

func (this *SeqKitSortOptions) BySeq(v bool) *SeqKitSortOptions {
	this.inner.BySeq = &v
	return this
}

func (this *SeqKitSortOptions) ByName(v bool) *SeqKitSortOptions {
	this.inner.ByName = &v
	return this
}

func (this *SeqKitSortOptions) ByLength(v bool) *SeqKitSortOptions {
	this.inner.ByLength = &v
	return this
}

func (this *SeqKitSortOptions) ByBases(v bool) *SeqKitSortOptions {
	this.inner.ByBases = &v
	return this
}

func (this *SeqKitSortOptions) GapLetters(v string) *SeqKitSortOptions {
	this.inner.GapLetters = &v
	return this
}

func (this *SeqKitSortOptions) Reverse(v bool) *SeqKitSortOptions {
	this.inner.Reverse = &v
	return this
}

func (this *SeqKitSortOptions) IgnoreCase(v bool) *SeqKitSortOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitSortOptions) SeqPrefixLength(v uint) *SeqKitSortOptions {
	this.inner.SeqPrefixLength = &v
	return this
}

func Sort(input *api.IDataFrame[string], o *SeqKitSortOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitSortOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	inNaturalOrder := *opts.InNaturalOrder
	bySeq := *opts.BySeq
	byName := *opts.ByName
	byLength := *opts.ByLength
	byBases := *opts.ByBases
	reverse := *opts.Reverse

	if byBases {
		byLength = true
		*opts.ByLength = true
	}

	n := 0
	if bySeq {
		n++
	}
	if byName {
		n++
	}
	if byLength {
		n++
	}
	if n > 1 {
		return nil, fmt.Errorf("only one of the options (byLength), (byName) and (bySeq) is allowed")
	}

	if byLength {
		parser, err := api.AddParam(libSource("SortParseInputInt"), "opts", OptionsToString(opts))
		if err != nil {
			return nil, err
		}
		conv, err := api.MapPartitions[string, ipair.IPair[int, string]](input, parser)
		if err != nil {
			return nil, err
		}
		sorted, err := api.ToPair[int, string](conv).SortByKey(!reverse, libSource("SortInt"))
		if err != nil {
			return nil, err
		}
		return api.Map[ipair.IPair[int, string], string](sorted.FromPair(), libSource("ValuesIntString"))

	} else {
		parser, err := api.AddParam(libSource("SortParseInputString"), "opts", OptionsToString(opts))
		if err != nil {
			return nil, err
		}
		conv, err := api.MapPartitions[string, ipair.IPair[string, string]](input, parser)
		if err != nil {
			return nil, err
		}
		sortName := "SortString"
		if !bySeq && inNaturalOrder {
			sortName = "SortNatural"
		}

		sorted, err := api.ToPair[string, string](conv).SortByKey(!reverse, libSource(sortName))
		if err != nil {
			return nil, err
		}
		return api.Map[ipair.IPair[string, string], string](sorted.FromPair(), libSource("ValuesStringString"))
	}
}
