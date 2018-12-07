# pylogabstract
This is accompanying code for a paper entitled "Automatic event log abstraction to support forensic investigation" submitted to DSN conference 2019. We name the proposed method as `pylogabstract`. To run the `pylogabstract` tool, please follow these steps. 

1. Clone the repository

   `git clone https://github.com/logforensicator/pylogabstract.git`

2. Change directory to `pylogabstract`

   `cd pylogabstract`

3. Make sure you have a `pipenv` installed and the run its shell

    `pipenv shell`

4. Install `pylogabstract`

   `pip install -e .`

5. Run `pylogabstract` to get abstractions from a log file such as `auth.log`

   `pylogabstract -i auth.log`

6. We can save abstraction results in an output file such as `abstractions-output.log`

   `pylogabstract -i auth.log -o auth-output.log`