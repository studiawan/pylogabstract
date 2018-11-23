from setuptools import setup

setup(name='pylogabstract',
      version='0.0.1',
      description='Event log abstraction in Python',
      long_description='This package contains event log abstraction method using Python',
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.5',
      ],
      keywords='log abstraction',
      url='http://github.com/studiawan/pylogabstract/',
      author='Hudan Studiawan',
      author_email='studiawan@gmail.com',
      license='MIT',
      packages=['pylogabstract'],
      entry_points={
          'console_scripts': [
              'pylogabstract = pylogabstract.pylogabstract:main'
          ],
      },
      install_requires=[
          'tensorflow==1.4.1',
          'networkx',
          'python-louvain'
      ],
      include_package_data=True,
      zip_safe=False)
