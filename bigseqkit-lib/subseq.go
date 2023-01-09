package main

import (
	"bigseqkit"
	"fmt"
	"github.com/shenwei356/bio/featio/gtf"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fai"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/breader"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	log "ignis/executor/core/logger"
	"io"
	"os"
	"strconv"
	"strings"
)

func NewSubseqTransform() any {
	return &SubseqTransform{}
}

type SubseqTransform struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts           bigseqkit.SubseqOptions
	alphabet       *seq.Alphabet
	start, end     int
	gtfFeaturesMap map[string]type2gtfFeatures
	bedFeatureMap  map[string][]BedFeature
}

func (this *SubseqTransform) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.SubseqOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	if err != nil {
		return err
	}
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false

	chrs2 := make([]string, len(*this.opts.Chr))
	for i, chr := range *this.opts.Chr {
		chrs2[i] = chr
	}
	*this.opts.Chr = chrs2
	chrsMap := make(map[string]struct{}, len(*this.opts.Chr))
	for _, chr := range *this.opts.Chr {
		chrsMap[strings.ToLower(chr)] = struct{}{}
	}

	choosedFeatures2 := make([]string, len(*this.opts.Feature))
	for i, f := range *this.opts.Feature {
		choosedFeatures2[i] = strings.ToLower(f)
	}
	*this.opts.Feature = choosedFeatures2

	if *this.opts.OnlyFlank {
		if *this.opts.UpStream > 0 && *this.opts.DownStream > 0 {
			return fmt.Errorf("when flag -f (--only-flank) given," +
				" only one of flags -u (--up-stream) and -d (--down-stream) is allowed")
		} else if *this.opts.UpStream == 0 && *this.opts.DownStream == 0 {
			return fmt.Errorf("when flag -f (--only-flank) given," +
				" one of flags -u (--up-stream) and -d (--down-stream) should be given")
		}
	}
	if *this.opts.Region != "" {
		if *this.opts.UpStream > 0 || *this.opts.DownStream > 0 || *this.opts.OnlyFlank {
			return fmt.Errorf("when flag -r (--region) given," +
				" any of flags -u (--up-stream), -d (--down-stream) and -f (--only-flank) is not allowed")
		}
	}

	if *this.opts.Region != "" {
		if !reRegion.MatchString(*this.opts.Region) {
			return fmt.Errorf(`invalid region: %s. type "seqkit subseq -h" for more examples`, *this.opts.Region)
		}
		r := strings.Split(*this.opts.Region, ":")
		this.start, err = strconv.Atoi(r[0])
		if err != nil {
			return err
		}
		this.end, err = strconv.Atoi(r[1])
		if err != nil {
			return err
		}
		if this.start == 0 || this.end == 0 {
			return fmt.Errorf("both start and end should not be 0")
		}
		if this.start < 0 && this.end > 0 {
			return fmt.Errorf("when start < 0, end should not > 0")
		}
	} else if *this.opts.Gtf != "" {
		if !*this.opts.Config.Quiet {
			log.Info("read GTF file ...")
		}
		this.gtfFeaturesMap = make(map[string]type2gtfFeatures)

		gtf.Threads = context.Threads() // threads of gtf.ReadFeatures
		var features []gtf.Feature
		if len(*this.opts.Chr) > 0 || len(*this.opts.Feature) > 0 {
			features, err = gtf.ReadFilteredFeatures(*this.opts.Gtf, *this.opts.Chr, *this.opts.Feature, []string{*this.opts.GtfTag})
		} else {
			features, err = gtf.ReadFilteredFeatures(*this.opts.Gtf, []string{}, []string{}, []string{*this.opts.GtfTag})
		}
		gtf.Threads = 1
		if err != nil {
			return err
		}

		var chr, feat string
		for _, feature := range features {
			chr = strings.ToLower(feature.SeqName)
			if _, ok := this.gtfFeaturesMap[chr]; !ok {
				this.gtfFeaturesMap[chr] = make(map[string][]gtf.Feature)
			}
			feat = strings.ToLower(feature.Feature)
			if _, ok := this.gtfFeaturesMap[chr][feat]; !ok {
				this.gtfFeaturesMap[chr][feat] = []gtf.Feature{}
			}
			this.gtfFeaturesMap[chr][feat] = append(this.gtfFeaturesMap[chr][feat], feature)
		}
		if !*this.opts.Config.Quiet {
			log.Info(fmt.Sprintf("%d GTF features loaded", len(features)))
		}
	} else if *this.opts.Bed != "" {
		if !*this.opts.Config.Quiet {
			log.Info("read BED file ...")
		}
		if len(*this.opts.Feature) > 0 {
			return fmt.Errorf("when given flag -b (--bed), flag -f (--feature) is not allowed")
		}
		this.bedFeatureMap = make(map[string][]BedFeature)

		var features []BedFeature
		if len(*this.opts.Chr) > 0 {
			features, err = ReadBedFilteredFeatures(*this.opts.Bed, *this.opts.Chr, context.Threads())
		} else {
			features, err = ReadBedFeatures(*this.opts.Bed, context.Threads())
		}
		if err != nil {
			return err
		}

		var chr string
		for _, feature := range features {
			chr = strings.ToLower(feature.Chr)
			if _, ok := this.bedFeatureMap[chr]; !ok {
				this.bedFeatureMap[chr] = []BedFeature{}
			}
			this.bedFeatureMap[chr] = append(this.bedFeatureMap[chr], feature)
		}
		if !*this.opts.Config.Quiet {
			log.Info(fmt.Sprintf("%d BED features loaded", len(features)))
		}
	} else {
		return fmt.Errorf("one of the options needed: -r/--region, --bed, --gtf")
	}

	return nil
}

func (this *SubseqTransform) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	fastxReader, err := NewSeqParser(this.alphabet, v1, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record

	result := make([]string, 0, 100)

	for {
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if fastxReader.IsFastq {
			*this.opts.Config.LineWidth = 0
			fastx.ForcelyOutputFastq = true
		}

		if *this.opts.Region != "" {
			result = append(result, string(subseqByRegion(record, *this.opts.Config.LineWidth, this.start, this.end)))

		} else if *this.opts.Gtf != "" {
			seqname := strings.ToLower(string(record.ID))
			if _, ok := this.gtfFeaturesMap[seqname]; !ok {
				continue
			}

			r, err := subseqByGTFFile(record, *this.opts.Config.LineWidth,
				this.gtfFeaturesMap, *this.opts.Feature,
				*this.opts.OnlyFlank, *this.opts.UpStream, *this.opts.DownStream, *this.opts.GtfTag)
			if err != nil {
				return nil, err
			}
			if r != nil {
				result = append(result, string(r))
			}

		} else if *this.opts.Bed != "" {
			seqname := strings.ToLower(string(record.ID))
			if _, ok := this.bedFeatureMap[seqname]; !ok {
				continue
			}
			r, err := subSeqByBEDFile(record, *this.opts.Config.LineWidth,
				this.bedFeatureMap,
				*this.opts.OnlyFlank, *this.opts.UpStream, *this.opts.DownStream)
			if err != nil {
				return nil, err
			}
			if r != nil {
				result = append(result, string(r))
			}
		}
	}
	return result, nil
}

// BedFeature is the gff BedFeature struct
type BedFeature struct {
	Chr    string
	Start  int // 1based
	End    int // end included
	Name   *string
	Strand *string
}

// ReadBedFeatures returns gtf BedFeatures of a file
func ReadBedFeatures(file string, threads int) ([]BedFeature, error) {
	return ReadBedFilteredFeatures(file, []string{}, threads)
}

// ReadBedFilteredFeatures returns gtf BedFeatures of selected chrs from file
func ReadBedFilteredFeatures(file string, chrs []string, threads int) ([]BedFeature, error) {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		return nil, err
	}
	chrsMap := make(map[string]struct{}, len(chrs))
	for _, chr := range chrs {
		chrsMap[chr] = struct{}{}
	}

	fn := func(line string) (interface{}, bool, error) {
		line = strings.TrimRight(line, "\r\n")

		if line == "" || line[0] == '#' || (len(line) > 7 && string(line[0:7]) == "browser") || (len(line) > 5 && string(line[0:5]) == "track") {
			return nil, false, nil
		}

		items := strings.Split(line, "\t")
		n := len(items)
		if n < 3 {
			return nil, false, nil
		}

		if len(chrs) > 0 { // selected chrs
			if _, ok := chrsMap[items[0]]; !ok {
				return nil, false, nil
			}
		}

		start, err := strconv.Atoi(items[1])
		if err != nil {
			return nil, false, fmt.Errorf("%s: bad start: %s", items[0], items[1])
		}
		end, err := strconv.Atoi(items[2])
		if err != nil {
			return nil, false, fmt.Errorf("%s: bad end: %s", items[0], items[2])
		}
		if start >= end {
			return nil, false, fmt.Errorf("%s: start (%d) must be <= end (%d)", items[0], start, end)
		}

		var name *string
		if n >= 4 {
			name = &items[3]
		}
		var strand *string
		if n >= 6 {
			if items[5] != "+" && items[5] != "-" && items[5] != "." {
				return nil, false, fmt.Errorf("bad strand: %s", items[5])
			}
			strand = &items[5]
		}

		return BedFeature{items[0], start + 1, end, name, strand}, true, nil
	}
	reader, err := breader.NewBufferedReader(file, threads, 100, fn)
	if err != nil {
		return nil, err
	}
	BedFeatures := make([]BedFeature, 0, 1024)
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			return nil, chunk.Err
		}
		for _, data := range chunk.Data {
			BedFeatures = append(BedFeatures, data.(BedFeature))
		}
	}
	return BedFeatures, nil
}

type type2gtfFeatures map[string][]gtf.Feature

func subseqByRegion(record *fastx.Record, lineWidth int, start, end int) []byte {
	record.Seq = record.Seq.SubSeq(start, end)
	return record.Format(lineWidth)
}

func subseqByGTFFile(record *fastx.Record, lineWidth int,
	gtfFeaturesMap map[string]type2gtfFeatures, choosedFeatures []string,
	onlyFlank bool, upStream int, downStream int, gtfTag string) ([]byte, error) {

	seqname := strings.ToLower(string(record.ID))

	var strand, tag, outname, flankInfo string
	var s, e int
	var subseq *seq.Seq

	featsMap := make(map[string]struct{}, len(choosedFeatures))
	for _, chr := range choosedFeatures {
		featsMap[chr] = struct{}{}
	}

	for featureType := range gtfFeaturesMap[seqname] {
		if len(choosedFeatures) > 0 {
			if _, ok := featsMap[strings.ToLower(featureType)]; !ok {
				continue
			}
		}
		for _, feature := range gtfFeaturesMap[seqname][featureType] {
			s, e = feature.Start, feature.End
			if feature.Strand != nil && *feature.Strand == "-" {
				if onlyFlank {
					if upStream > 0 {
						s = feature.End + 1
						e = feature.End + upStream
					} else {
						s = feature.Start - downStream
						e = feature.Start - 1
					}
				} else {
					s = feature.Start - downStream // seq.SubSeq will check it
					e = feature.End + upStream
				}
				if s < 1 {
					s = 1
				}
				if e > len(record.Seq.Seq) {
					e = len(record.Seq.Seq)
				}
				subseq = record.Seq.SubSeq(s, e).RevComInplace()
			} else {
				if onlyFlank {
					if upStream > 0 {
						s = feature.Start - upStream
						e = feature.Start - 1
					} else {
						s = e + 1
						e = e + downStream
					}
				} else {
					s = feature.Start - upStream
					e = feature.End + downStream
				}
				if s < 1 {
					s = 1
				}
				if e > len(record.Seq.Seq) {
					e = len(record.Seq.Seq)
				}
				subseq = record.Seq.SubSeq(s, e)
			}

			if feature.Strand == nil {
				strand = "."
			} else {
				strand = *feature.Strand
			}
			tag = ""
			for _, arrtribute := range feature.Attributes {
				if arrtribute.Tag == gtfTag {
					tag = arrtribute.Value
					break
				}
			}
			if upStream > 0 {
				if onlyFlank {
					flankInfo = fmt.Sprintf("_usf:%d", upStream)
				} else if downStream > 0 {
					flankInfo = fmt.Sprintf("_us:%d_ds:%d", upStream, downStream)
				} else {
					flankInfo = fmt.Sprintf("_us:%d", upStream)
				}
			} else if downStream > 0 {
				if onlyFlank {
					flankInfo = fmt.Sprintf("_dsf:%d", downStream)
				} else if upStream > 0 {
					flankInfo = fmt.Sprintf("_us:%d_ds:%d", upStream, downStream)
				} else {
					flankInfo = fmt.Sprintf("_ds:%d", downStream)
				}
			} else {
				flankInfo = ""
			}
			outname = fmt.Sprintf("%s_%d-%d:%s%s %s", record.ID, feature.Start, feature.End, strand, flankInfo, tag)
			var newRecord *fastx.Record
			var err error
			if len(subseq.Qual) > 0 {
				newRecord, err = fastx.NewRecordWithQualWithoutValidation(record.Seq.Alphabet, []byte(outname), []byte(outname), []byte{}, subseq.Seq, subseq.Qual)
			} else {
				newRecord, err = fastx.NewRecordWithoutValidation(record.Seq.Alphabet, []byte(outname), []byte(outname), []byte{}, subseq.Seq)
			}
			if err != nil {
				return nil, err
			}
			return newRecord.Format(lineWidth), nil
		}
	}
	return nil, nil
}

func subSeqByBEDFile(record *fastx.Record, lineWidth int,
	bedFeatureMap map[string][]BedFeature,
	onlyFlank bool, upStream, downStream int) ([]byte, error) {
	seqname := strings.ToLower(string(record.ID))

	var strand, geneID, outname, flankInfo string
	var s, e int
	var subseq *seq.Seq
	for _, feature := range bedFeatureMap[seqname] {
		s, e = feature.Start, feature.End
		if feature.Strand != nil && *feature.Strand == "-" {
			if onlyFlank {
				if upStream > 0 {
					s = feature.End + 1
					e = feature.End + upStream
				} else {
					s = feature.Start - downStream
					e = feature.Start - 1
				}
			} else {
				s = feature.Start - downStream // seq.SubSeq will check it
				e = feature.End + upStream
			}
			if s < 1 {
				s = 1
			}
			if e > len(record.Seq.Seq) {
				e = len(record.Seq.Seq)
			}
			subseq = record.Seq.SubSeq(s, e).RevComInplace()
		} else {
			if onlyFlank {
				if upStream > 0 {
					s = feature.Start - upStream
					e = feature.Start - 1
				} else {
					s = e + 1
					e = e + downStream
				}
			} else {
				s = feature.Start - upStream
				e = feature.End + downStream
			}
			if s < 1 {
				s = 1
			}
			if e > len(record.Seq.Seq) {
				e = len(record.Seq.Seq)
			}
			subseq = record.Seq.SubSeq(s, e)
		}

		if feature.Strand == nil {
			strand = "."
		} else {
			strand = *feature.Strand
		}
		geneID = ""
		if feature.Name != nil {
			geneID = *feature.Name
		}
		if upStream > 0 {
			if onlyFlank {
				flankInfo = fmt.Sprintf("_usf:%d", upStream)
			} else if downStream > 0 {
				flankInfo = fmt.Sprintf("_us:%d_ds:%d", upStream, downStream)
			} else {
				flankInfo = fmt.Sprintf("_us:%d", upStream)
			}
		} else if downStream > 0 {
			if onlyFlank {
				flankInfo = fmt.Sprintf("_dsf:%d", downStream)
			} else if upStream > 0 {
				flankInfo = fmt.Sprintf("_us:%d_ds:%d", upStream, downStream)
			} else {
				flankInfo = fmt.Sprintf("_ds:%d", downStream)
			}
		} else {
			flankInfo = ""
		}
		outname = fmt.Sprintf("%s_%d-%d:%s%s %s", record.ID, feature.Start, feature.End, strand, flankInfo, geneID)
		var newRecord *fastx.Record
		var err error
		if len(subseq.Qual) > 0 {
			newRecord, err = fastx.NewRecordWithQualWithoutValidation(record.Seq.Alphabet, []byte(outname), []byte(outname), []byte{}, subseq.Seq, subseq.Qual)
		} else {
			newRecord, err = fastx.NewRecordWithoutValidation(record.Seq.Alphabet, []byte(outname), []byte(outname), []byte{}, subseq.Seq)
		}
		if err != nil {
			return nil, err
		}
		return newRecord.Format(lineWidth), nil
	}
	return nil, nil
}
