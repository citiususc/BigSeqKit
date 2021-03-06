=========
BigSeqKit
=========
The Next Generation Sequencing (NGS) raw data are stored in FASTA and FASTQ text-based file formats. In this way, manipulating these files efficiently is essential to analyze and interpret data in any genomics pipeline. Common operations on FASTA/Q files include searching, filtering, sampling, deduplication and sorting, among others. We can find several tools in the literature for FASTA/Q file manipulation but none of them are well fitted for large files of tens of GB (likely TBs in the near future) since mostly they are based on sequential processing. The exception is *SeqKit* that allows some routines to use a few threads but, in any case, the scalability is very limited.

To deal with this issue, we introduce **BigSeqKit**, a parallel toolkit to manipulate FASTA/Q files at scale with speed and scalability at its core. **BigSeqKit** takes advantage of an HPC-Big Data framework (`IgnisHPC <https://ignishpc.readthedocs.io>`_) to parallelize and optimize the commands included in `SeqKit <https://github.com/shenwei356/seqkit>`_. In this way, in most cases **it is from tens to hundreds of times faster than SeqKit**. At the same time, our tool is easy to use and install on any kind of hardware platform (single server or cluster).

In order to improve the usability and facilitate the adoption of **BigSeqKit**,
it implements the same command interface than `SeqKit <https://bioinf.shenwei.me/seqkit/usage>`_.

------------
User's Guide
------------

BigSeqKit example
~~~~~~~~~~~~~~~~~


.. code-block:: go

	package main

	import (
		"ignis/driver/api"
		"bigseqkit"
	)
	// Auxiliary function for error checking, driver aborts if err is not nil
	func check[T any](e T, err error) T {if err != nil { panic(err) } else { return e }}

	func main() {
		// Initialization of the framework
		check(0, api.Ignis.Start())
		// Stop the framework when main ends
		defer api.Ignis.Stop()
		// Resources/Configuration of the cluster
		prop := check(api.NewIProperties())
		check(prop.Set("ignis.executor.image", "ignishpc/full"))
		check(prop.Set("ignis.executor.instances", "2"))
		check(prop.Set("ignis.executor.cores", "4"))
		check(prop.Set("ignis.executor.memory", "6GB"))
		// Construction of the cluster
		cluster := check(api.NewIClusterProps(prop))
		// Initialization of a Go Worker
		worker := check(api.NewIWorkerDefault(cluster, "go"))
		// Sequence reading
		seqs := check(bigSeqKit.ReadFASTA("mysequences.fa", worker))
		// Deletion of duplicated sequences
		rm_opts := &bigSeqKit.SeqKitRmDupOptions{}
		rm_opts.BySeq(true)
		u_seqs := check(bigSeqKit.RmDup(seqs, rm_opts))
		// Sort by ID
		sort_opts := &bigSeqKit.SeqKitSortOptions{}
		u_sorted_seqs := check(bigSeqKit.Sort(u_seqs, sort_opts))
		// Save the result
		check(0, u_sorted_seqs.SaveAsTextFile("result.fa"))
	}

Code above shows an example of a IgnisHPC driver implemented in Go that executes *BigSeqKit* *rmdup* and *sort* commands. *BigSeqKit* has been created as a **library**, so it only needs to be imported to be used within IgnisHPC (line 5). Instead of commands from terminal like *SeqKit*, *BigSeqKit* utilities are functions that can be called from a driver code. Note that their names and arguments are exactly the same than those included in *SeqKit*, which can be found in `https://bioinf.shenwei.me/seqkit/usage`.

Functions in *BigSeqKit* do not use files as input, they use DataFrames instead, an abstract representation of parallel data used by IgnisHPC (similar to RDDs in Spark). Parameters are grouped in a data structure where each field represents the long names of a parameter. Note that *BigSeqKit* functions can be linked (like system pipes using "|"), so the DataFrame generated by one can be used as input to another. In this way, integrate *BigSeqKit* routines in a more complex code is really easy.

After initializing the IgnisHPC framework, a cluster of containers is configured and built (lines from 12 to 24 in the Figure. Multiple parameters can be used to configure the environment such as image, number of containers, number of cores and memory per container. In this example, we will use a 2 nodes cluster (instances) and 4 cores by node. After configuring the IgnisHPC execution environment, the *BigSeqKit* code actually starts. First, we read the input file (line 26). There is a different function for reading FASTA and FASTQ files. All the input sequences are stored as a single data structure. The next stage consists of the elimination of the duplicated sequences using *rmdup* (lines from 28 to 30). The function takes as parameters the sequences and the options that specify its behavior. In particular, the option used is set in line 29, and its equivalent to the *SeqKit* parameter ``--by-seq``. The result of the *rmdup* operation is now sorted (lines 32-33) using the default options. Finally, the sorted sequences are written to disk. It is important to highlight that lazy evaluation is performed, so functions are only executed when the result is required to be saved on disk.

Running an IgnisHPC job
~~~~~~~~~~~~~~~~~~~~~~~


In this section we will explain the steps necessary to run the *BigSeqKit* example. IgnisHPC can be executed on different execution environments. In this case, we will focus on two common scenarios: a deployment on a local computer using Docker, and a deployment on a HPC cluster that uses Slurm as workload manager. For more details, IgnisHPC has an online documentation available for users `https://ignishpc.readthedocs.io`.

Installation of IgnisHPC (only once)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The only dependence of IgnisHPC is Docker, so its installation on the local system is mandatory (please refer to `Docker <https://docs.docker.com/get-docker/>`_ for instructions).

Next, we will install the ``ignis-deploy`` script using ``pip``:

.. code-block:: sh

	pip install ignishpc



IgnisHPC is a framework that works inside containers, so it is necessary to build the required images. Next, we show the corresponding commands to do it. IgnisHPC supports C/C++, Python, Java and Go programming languages, but since the code of Figure \ref{fig:example} was implemented using only Go, it is only necessary to build the *core-go* image. There are the equivalent *core-java*, *core-cpp* and *core-python* images.


.. code-block:: sh

	ignis-deploy images build --platform amd64,ppc64le --full --ignore submitter mesos nomad zookeeper --sources\
	   https://github.com/ignishpc/dockerfiles.git \
	   https://github.com/ignishpc/backend.git \
	   https://github.com/ignishpc/core-go.git \
	   https://github.com/citiususc/BigSeqKit.git


Note that the ``--platform`` parameter is used to specify the target processor architecture. In the example, we build images for *amd64* architectures and those based on PowerPC processors such as the Marconi100 supercomputer. If this parameter is not specified, the target architecture will be the one where the command is executed on.

It is important to highlight that the above commands must be executed only once.


Compilation of the example code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To compile the example code, the following command should be executed:

.. code-block:: sh

	docker run --rm -v <example-dir>:/src -w /src ignishpc/go-libs-compiler igo-bigseqkit-build


Go programming language *compiles folders* instead of particular files, so the example code should be stored inside ``<example-dir>``.

Execution on a local computer (Docker)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sh

	# Extract ignis-submit for use without a container
	docker run --rm -v $(pwd):/target ignishpc/submitter ignis-export /target

	# set current directory as job directory
	export IGNIS_DFS_ID=$(pwd)
	# set docker as scheduler
	export IGNIS_SCHEDULER_TYPE=docker
	# set where docker is available
	export IGNIS_SCHEDULER_URL=/var/run/docker.sock

	# Submit the job
	./ignis/bin/ignis-submit ignishpc/full ./example



First, we obtain the submit script ``ignis-submit`` to use it locally (line 2). Then the script is configured because it can work with multiple schedulers. In this case, we set up "docker" as scheduler and its path is defined (lines 7-9). On the other hand, IgnisHPC always needs a job directory. In this case, we use the current directory (line 5). Finally, the job is launched (line 12).

Execution on an HPC cluster (Slurm and Singularity)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sh

	# Create the Singularity image
	ignis-deploy images singularity --host ignishpc/full ignis_full.sif

	# Extract ignis-slurm for use without a container
	docker run --rm -v $(pwd):/target ignishpc/slurm-submitter ignis-export /target

	# Set current directory as job directory
	export IGNIS_DFS_ID=$(pwd)

	# Submit the job
	./ignis/bin/ignis-slurm 0:10:00 ignis_full.sif ./example


Normally HPC clusters do not support Docker for security issues, so they have installed `Singularity <https://sylabs.io/singularity/>`_ instead. As a result, we must convert ``ignishpc/full`` to a Singularity container (line 2). Next, we obtain the submit script ``ignis-slurm`` (line 5). Job directory is set (line 8) and the job is launched (line 11). Note that, unlike ``ignis-submit``, the Slurm script requires an estimation of the execution time in the format HH:MM:SS.
