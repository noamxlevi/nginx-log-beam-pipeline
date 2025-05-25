from setuptools import setup

setup(
    py_modules=['main', 'utils', 'pipeline'], 
    install_requires=[
        'apache-beam[gcp]', 
        'google-cloud-bigquery',  
    ],
    include_package_data=True,  
)
