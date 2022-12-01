package bigseqkit

import (
	"ignis/driver/api"
)

type SeqKitFaidxOptions struct {
	inner FaidxOptions
}

type FaidxOptions struct {
	Config     KitConfig
	UseRegexp  *bool
	IgnoreCase *bool
	FullHead   *bool
	RegionFile *string
	Regions    *[]string
}

func (this *FaidxOptions) setDefaults() *FaidxOptions {
	this.Config.setDefaults()
	setDefault(&this.UseRegexp, false)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.FullHead, false)
	setDefault(&this.RegionFile, "")
	setDefault(&this.Regions, make([]string, 0))

	return this
}

func (this *SeqKitFaidxOptions) Config(v *SeqKitConfig) *SeqKitFaidxOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitFaidxOptions) UseRegexp(v bool) *SeqKitFaidxOptions {
	this.inner.UseRegexp = &v
	return this
}

func (this *SeqKitFaidxOptions) IgnoreCase(v bool) *SeqKitFaidxOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitFaidxOptions) FullHead(v bool) *SeqKitFaidxOptions {
	this.inner.FullHead = &v
	return this
}

func (this *SeqKitFaidxOptions) RegionFile(v string) *SeqKitFaidxOptions {
	this.inner.RegionFile = &v
	return this
}

func (this *SeqKitFaidxOptions) Regions(v []string) *SeqKitFaidxOptions {
	this.inner.Regions = &v
	return this
}

func Faidx(input *api.IDataFrame[string], o *SeqKitFaidxOptions) (
	idx *api.IDataFrame[string], queries *api.IDataFrame[string], err error) {
	if o == nil {
		o = &SeqKitFaidxOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	offsets, err := api.MapPartitions[string, int64](input, libSource("FaidxOffset"))
	if err != nil {
		return nil, nil, err
	}
	offsetsArray, err := offsets.Collect()
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < len(offsetsArray)-1; i++ {
		offsetsArray[i+1] = offsetsArray[i]
	}
	offsetsArray[0] = 0

	libfaidx, err := api.AddParam(libSource("Faidx"), "offsets", offsetsArray)
	if err != nil {
		return nil, nil, err
	}

	libfaidx, err = api.AddParam(libfaidx, "opts", OptionsToString(opts))
	if err != nil {
		return nil, nil, err
	}

	faidx, err := api.MapPartitionsWithIndex[string, string](input, libfaidx)
	if err != nil {
		return nil, nil, err
	}

	if len(*opts.Regions) == 0 && *opts.RegionFile == "" {
		return faidx, nil, err
	}

	libqueries, err := api.AddParam(libSource("FaidxQuery"), "opts", OptionsToString(opts))
	if err != nil {
		return faidx, nil, err
	}

	queries, err = api.MapPartitions[string, string](input, libqueries)
	return faidx, queries, err
}
