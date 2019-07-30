from setuptools import setup

setup(
    name='awsbw',
    version='0.0.5',
    description="""A small CLI utility to view jobs in an AWS batch queue
      """,
    url='https://github.com/jgolob/awsbw',
    author='Jonathan Golob',
    author_email='j-dev@golob.org',
    license='MIT',
    packages=['awsbw'],
    zip_safe=False,
    install_requires=[
        'boto3',
    ],
    entry_points={
        'console_scripts': ['awsbw=awsbw.awsbw:main'],
    }
)
