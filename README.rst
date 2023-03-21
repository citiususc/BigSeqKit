=========
BigSeqKit
=========
The Next Generation Sequencing (NGS) raw data are stored in FASTA and FASTQ text-based file formats. In this way, manipulating these files efficiently is essential to analyze and interpret data in any genomics pipeline. Common operations on FASTA/Q files include searching, filtering, sampling, deduplication and sorting, among others. We can find several tools in the literature for FASTA/Q file manipulation but none of them are well fitted for large files of tens of GB (likely TBs in the near future) since mostly they are based on sequential processing. The exception is `seqkit <https://github.com/shenwei356/seqkit>`_ that allows some routines to use a few threads but, in any case, the scalability is very limited.

To deal with this issue, we introduce **BigSeqKit**, a parallel toolkit to manipulate FASTA/Q files at scale with speed and scalability at its core. *BigSeqKit* takes advantage of an HPC-Big Data framework (`IgnisHPC <https://ignishpc.readthedocs.io>`_) to parallelize and optimize the commands included in *seqkit*. In this way, in most cases **it is from tens to hundreds of times faster than other state-of-the-art tools** such as *seqkit*, `samtools <https://www.htslib.org>`_ and `pyfastx <https://pyfastx.readthedocs.io/en/latest>`_. At the same time, our tool is easy to use and install on any kind of hardware platform (single server or cluster). Routines in *BigSeqKit* can be used as a bioinformatics library or from the command line.

In order to improve the usability and facilitate the adoption of *BigSeqKit*, it implements the same command interface than `SeqKit <https://bioinf.shenwei.me/seqkit/usage>`_.

------------
User's Guide
------------

BigSeqKit from the command line (CLI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BigSeqKit (and IgnisHPC) can be executed on different execution environments. In this case, we will focus on two common scenarios: running on a local computer, and a deployment on a HPC cluster that uses Slurm as workload manager. For more details, IgnisHPC has an online documentation available for users `https://ignishpc.readthedocs.io`.

First, we will install the ``ignis-deploy`` script using ``pip`` (required only first time):

.. code-block:: sh

	pip install ignishpc

Local server (Docker)
^^^^^^^^^^^^^^^^^^^^^

To execute *BigSeqKit* on a local server is necessary to install Docker (please refer to `Docker <https://docs.docker.com/get-docker/>`_ for instructions).

Download the precompiled IgnisHPC image (required only first time):

.. code-block:: sh

	docker pull ignishpc/full

Extract ``ignis-submit`` script to use it without a container (required only first time):

.. code-block:: sh

	docker run --rm -v $(pwd):/target ignishpc/submitter ignis-export /target

Set the following environment variables:

.. code-block:: sh

	# set current directory as job directory
	export IGNIS_DFS_ID=$(pwd)
	# set docker as scheduler
	export IGNIS_SCHEDULER_TYPE=docker
	# set where docker is available
	export IGNIS_SCHEDULER_URL=/var/run/docker.sock

Now it is only necessary to select command or routine (see a complete list `here <https://bioinf.shenwei.me/seqkit/usage>`_) and pass its arguments through command line following the syntax:

.. code-block:: sh

	./ignis/bin/ignis-submit ignishpc/full bigseqkit <cmd> <arguments>

For example, the following expression uses the routine *seq* to print the name of the sequences included in a FASTA file to an output file:

.. code-block:: sh

	./ignis/bin/ignis-submit ignishpc/full bigseqkit seq -n -o names.txt input-file.fa

HPC Cluster (Slurm and Singularity)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We assume that the cluster has installed Slurm and Singularity.

Create the Singularity image on your local server (required only first time):

.. code-block:: sh

	ignis-deploy images singularity --host ignishpc/full ignis_full.sif

Extract ``ignis-slurm`` to use it without a container (required only first time):

.. code-block:: sh

	docker run --rm -v $(pwd):/target ignishpc/slurm-submitter ignis-export /target

Move the Singularity image and the ``ignis/`` folder to the cluster.

In the cluster, set the following environment variables:

.. code-block:: sh

	# set current directory as job directory
	export IGNIS_DFS_ID=$(pwd)

Now it is only necessary to select command or routine (see a complete list `here <https://bioinf.shenwei.me/seqkit/usage>`_) and pass its arguments through command line following the syntax:

.. code-block:: sh

	./ignis/bin/ignis-slurm HH:MM:SS ignis_full.sif bigseqkit <cmd> <arguments>

Note that, unlike ``ignis-submit``, the Slurm script requires an estimation of the execution time in the format HH:MM:SS.

For example, the following expression uses the routine *seq* to print the name of the sequences included in a FASTA file to an output file:

.. code-block:: sh

	./ignis/bin/ignis-slurm HH:MM:SS ignis_full.sif bigseqkit seq -n -o names.txt input-file.fa

Setting the number of computing nodes, cores and memory per node
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Users can also specify through arguments the number of instances (nodes), cores and memory (in GB) per node to be used in the execution. By default, those values are set to 1. For example, we can execute the previous command on a single server using 4 cores:

.. code-block:: sh

	./ignis/bin/ignis-submit ignishpc/full -p ignis.executor.cores=4 bigseqkit seq -n -o names.txt input-file.fa


BigSeqKit as a library
~~~~~~~~~~~~~~~~~~~~~~

*BigSeqKit* can also be used as a bioinformatics library. It is worth noting that *BigSeqKit* was implemented in Go language. However, thanks to the multi-language support provided by IgnisHPC, it is possible to call *BigSeqKit* routines from C/C++, Python, Java and Go applications without additional overhead. An example of Python code is shown below:

.. code-block:: python

	#!/bin/env python3
	
	import ignis
	import bigseqkit

	# Initialization of the framework
	ignis.Ignis.start()
	# Resources/Configuration of the cluster
	prop = ignis.IProperties()
	prop["ignis.executor.image"] = "ignishpc/full"
	prop["ignis.executor.instances"] = "2"
	prop["ignis.executor.cores"] = "4"
	prop["ignis.executor.memory"] = "1GB"
	# Construction of the cluster
	cluster = ignis.ICluster(prop)
	# Initialization of a Go Worker
	worker = ignis.IWorker(cluster, "go")
	# Sequence reading
	seqs = bigseqkit.readFASTA("file.fa", worker)
	# Obtain Sequence names
	names = bigseqkit.seq(seqs, name=True)
	# Save the result
	names.saveAsTextFile("names.txt")
	# Stop the framework
	ignis.Ignis.stop()

Instead of commands from terminal like *SeqKit*, *BigSeqKit* utilities are functions that can be called from a driver code. Note that their names and arguments are exactly the same than those included in *SeqKit*, which can be found in `https://bioinf.shenwei.me/seqkit/usage`.

Functions in *BigSeqKit* do not use files as input, they use DataFrames instead, an abstract representation of parallel data used by IgnisHPC (similar to RDDs in Spark). Parameters are grouped in a data structure where each field represents the long names of a parameter. Note that *BigSeqKit* functions can be linked (like system pipes using "|"), so the DataFrame generated by one can be used as input to another. In this way, integrate *BigSeqKit* routines in a more complex code is really easy.

The code starts initializing the IgnisHPC framework (line 5). Next, a cluster of containers is configured and built (lines from 7 to 15). Multiple parameters can be used to configure the environment such as image, number of containers, number of cores and memory per container. In this example, we will use 2 nodes (instances) and 4 cores per node. After configuring the IgnisHPC execution environment, the *BigSeqKit* code actually starts. First, we read the input file (line 17). There is a different function for reading FASTA and FASTQ files. All the input sequences are stored as a single data structure. The next stage consists of printing the name of the sequences included in the FASTA file (line 19). The function takes as parameters the sequences and the options that specify its behavior. Finally, the names of the sequences are written to disk.

Local server (Docker)
^^^^^^^^^^^^^^^^^^^^^

Download the precompiled IgnisHPC image (only first time):

.. code-block:: sh

	docker pull ignishpc/full

Extract ``ignis-submit`` for use without a container (only first time):

.. code-block:: sh

	docker run --rm -v $(pwd):/target ignishpc/submitter ignis-export /target

.. code-block:: sh

	# set current directory as job directory
	export IGNIS_DFS_ID=$(pwd)
	# set docker as scheduler
	export IGNIS_SCHEDULER_TYPE=docker
	# set where docker is available
	export IGNIS_SCHEDULER_URL=/var/run/docker.sock

	# Submit the job
	./ignis/bin/ignis-submit ignishpc/full ./example


HPC Cluster (Slurm and Singularity)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sh

	# Create the Singularity image (only first time)
	ignis-deploy images singularity --host ignishpc/full ignis_full.sif

	# Extract ignis-slurm for use without a container (only first time)
	docker run --rm -v $(pwd):/target ignishpc/slurm-submitter ignis-export /target

	# Set current directory as job directory
	export IGNIS_DFS_ID=$(pwd)

	# Submit the job
	./ignis/bin/ignis-slurm 0:10:00 ignis_full.sif ./example

As we mentioned previously, unlike ``ignis-submit``, the Slurm script requires an estimation of the execution time in the format HH:MM:SS.

Compilation of Go user code
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To compile user code implemented in Go instead of Python, the following command should be executed:

.. code-block:: sh

	docker run --rm -v <example-dir>:/src -w /src ignishpc/go-libs-compiler igo-bigseqkit-build

Go programming language *compiles folders* instead of particular files, so the example code should be stored inside ``<example-dir>``.

Installation from repository of BigSeqKit and IgnisHPC (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of using the preconfigured images uploaded to docker hub (x64 architecture), we can build ours locally. The only dependence of *BigSeqKit* is IgnisHPC, but at the same time, IgnisHPC depends on Docker, so its installation on the local system is mandatory (please refer to `Docker <https://docs.docker.com/get-docker/>`_ for instructions).

Next, we will install the ``ignis-deploy`` script using ``pip``:

.. code-block:: sh

	pip install ignishpc

IgnisHPC is a framework that works inside containers, so it is necessary to build the required images. Next, we show the corresponding commands to do it. IgnisHPC supports C/C++, Python, Java and Go programming languages, but since the example below was implemented using only Python, it is only necessary to build the *core-python* image. There are the equivalent *core-java*, *core-cpp* and *core-go* images.

.. code-block:: sh

	ignis-deploy images build --full --ignore submitter mesos nomad zookeeper --sources\
	   https://github.com/ignishpc/dockerfiles.git \
	   https://github.com/ignishpc/backend.git \
	   https://github.com/ignishpc/core-python.git \
	   https://github.com/citiususc/BigSeqKit.git


Note that the ``--platform`` parameter is used to specify the target processor architecture. Currently, we can build images for *amd64* systems and those based on PowerPC processors (*ppc64le*) such as the Marconi100 supercomputer (CINECA, Italy). If this parameter is not specified, the target architecture will be the one where the command is executed on.
