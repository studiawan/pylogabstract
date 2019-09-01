# pylogabstract
This is accompanying code for a paper entitled "Automatic event log abstraction to support forensic investigation". We name the proposed method as `pylogabstract`.

## Requirements
1. Python 3.5
2. TensorFlow 1.4.1
3. NetworkX 2.1
4. python-louvain 0.11

## How to run
To run the `pylogabstract` tool, please follow these steps.

1. Clone the repository

   `git clone https://github.com/studiawan/pylogabstract.git`

2. Change directory to `pylogabstract`

   `cd pylogabstract`

3. Make sure you have a `pipenv` installed and then run its shell on directory `pylogabstract`

    `pipenv shell`

4. If you are unable to use `pipenv`, create virtual environment using anaconda:

    `conda create --name pylogabstract python=3.5`
    
   and then activate it:
    
    `conda activate pylogabstract`
   
   If you do not have anaconda, use any other tools to create virtual environment. We highly recommend to install `pylogabstract` on a virtual environment.

5. Install `pylogabstract`

   `pip install -e .`

6. Run `pylogabstract` to get abstractions from a log file such as `auth.log`

   `pylogabstract -i /var/log/auth.log`

7. We can save abstraction results in an output file such as `auth-output.log`

   `pylogabstract -i /var/log/auth.log -o auth-output.log`

## Download the datasets

Please follow the instructions in [datasets/README.md](https://github.com/studiawan/pylogabstract/tree/master/datasets) to download and copy the datasets to the proper directory.

## Reproduce results from the paper

There are two scripts to run for reproducing results from the paper. First, we need to build ground truth. Second, we have to run the experiments. 

### (Optional) Build the ground truth. 

This is an optional step as we have included the ground truth in the `datasets` directory. However, if you want to build the ground truth by your own, follow these steps. 

1. Change some values in `datasets.conf` specifically `base_dir`, `perabstraction_dir`, `lineid_abstractionid_dir`, and `abstraction_withid_dir` based on your own directory structure in your computer

2. In the project root directory, run script `groundtruth.py` followed by dataset name. The dataset names supported are `['casper-rw', 'dfrws-2009-jhuisi', 'dfrws-2009-nssal','dfrws-2016', 'honeynet-challenge7']`. For example:

   `python pylogabstract/groundtruth/groundtruth.py casper-rw`

### Run the experiments

To run the experiments, follow these steps. 

1. Change some values in `abstraction.conf` specifically `dataset_path` and `result_path` based on your own directory structure in your computer

2. Run script `experiment.py` followed by method name and dataset name in the project root directory. For example:

   `python pylogabstract/experiment/experiment.py pylogabstract casper-rw`
