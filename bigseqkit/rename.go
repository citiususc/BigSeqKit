package bigseqkit

import (
	"ignis/driver/api"
	"ignis/executor/api/ipair"
)

type SeqKitRenameOptions struct {
	inner RenameOptions
}

type RenameOptions struct {
	Config KitConfig
	ByName *bool
}

func (this *RenameOptions) setDefaults() *RenameOptions {
	this.Config.setDefaults()
	setDefault(&this.ByName, false)

	return this
}

func (this *SeqKitRenameOptions) Config(v *SeqKitConfig) *SeqKitRenameOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitRenameOptions) ByName(v bool) *SeqKitRenameOptions {
	this.inner.ByName = &v
	return this
}

func Rename(input *api.IDataFrame[string], o *SeqKitRenameOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitRenameOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	libprepare, err := api.AddParam(libSource("RenamePrepare"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	prepared, err := api.MapPartitions[string, ipair.IPair[string, string]](input, libprepare)
	if err != nil {
		return nil, err
	}

	ready, err := api.GroupByKey[string, string](api.ToPair[string, string](prepared), nil)
	if err != nil {
		return nil, err
	}

	librename, err := api.AddParam(libSource("Rename"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.Flatmap[ipair.IPair[string, []string], string](ready.FromPair(), librename)
}
