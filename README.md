# pylogabstract
This is accompanying code for a paper entitled "Automatic event log abstraction to support forensic investigation" submitted to DSN conference 2019. We name the proposed method as `pylogabstract`.  

## Requirements
1. Python 3.5
2. TensorFlow 1.4.1
3. NetworkX 2.1
4. python-louvain 0.11

## How to run
To run the `pylogabstract` tool, please follow these steps.

1. Clone the repository

   `git clone https://github.com/logforensicator/pylogabstract.git`

2. Change directory to `pylogabstract`

   `cd pylogabstract`

3. Make sure you have a `pipenv` installed and then run its shell

    `pipenv shell`

4. Install `pylogabstract`

   `pip install -e .`

5. Run `pylogabstract` to get abstractions from a log file such as `auth.log`

   `pylogabstract -i /var/log/auth.log`

6. We can save abstraction results in an output file such as `auth-output.log`

   `pylogabstract -i /var/log/auth.log -o auth-output.log`