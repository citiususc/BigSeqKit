module seqkit-lib

go 1.18

require (
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/shenwei356/bio v0.7.0
	github.com/shenwei356/breader v0.3.1
	github.com/shenwei356/bwt v0.6.0
	github.com/shenwei356/natsort v0.0.0-20190418160752-600d539c017d
	ignis v0.0.0
	seqkit v0.0.0
)

require (
	github.com/apache/thrift v0.15.0 // indirect
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/klauspost/pgzip v1.2.5 // indirect
	github.com/pierrec/xxHash v0.1.5 // indirect
	github.com/shenwei356/util v0.5.0 // indirect
	github.com/shenwei356/xopen v0.2.1 // indirect
	github.com/twotwotwo/sorts v0.0.0-20160814051341-bf5c1f2b8553 // indirect
	github.com/ulikunitz/xz v0.5.10 // indirect
	golang.org/x/sys v0.0.0-20210927094055-39ccf1dd6fa6 // indirect
)

replace seqkit => ../seqkit

replace ignis => /home/cesar/core-go/ignis
