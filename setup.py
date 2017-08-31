from setuptools import setup, find_packages

setup(name='sequelize_csv',
      version='1.0.0',
      description='Sequelize CSV imports CSV files in SQLite.',
      url='http://github.com/akash314/sequelize_csv',
      author='Akash Agarwal',
      author_email='agarwala989@gmail.com',
      license='MIT',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'sequelize_csv = sequelize_csv.__main__:cli_run',
          ],
      },
      install_requires=['docopt'],
      zip_safe=False)
