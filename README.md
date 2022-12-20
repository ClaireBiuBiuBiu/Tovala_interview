# Tovala_interview
NewsAPI project for Tovala
Includes two parts: Python command line application and data analysis from results


## 1 Prerequisite
1 Install those packages into your machine (requests==2.28.1, boto3==1.15.18, snowflake.connector, json)

2 documentation for snowflake.connector download: https://docs.snowflake.com/en/user-guide/python-connector-install.html


## 2 Usage of TovalaTools 
 1 After getting packages installed, download python file "TovalaAPI.py", "TovalaAPI_run.py" and "TovalaAPI_cleanup.py" and place in the folder you like
 
 2 User interaction Version
  2.1 If you want user interaction, run command "python3 TovalaAPI.py" to execute the program inside the folder you chose 
 
  2.2 You have option to run, cleanup or exit the program
 
 3 Production Version
  3.1 Run "python3 TovalaAPI_run.py" to generate files in s3 and snowflake
  3.2 Run "python3 TovalaAPI_cleanup.py" to cleanup your workspace in s3 and snowflake


## 3 Program implementation thinking process
1 Use OOP to write a class containing multiple methods to implement to improve the convenience for users

2 Since the data I have access to only have dates till 12/12/2022, so I decided to make the API extrat time set as 12/12/2022 for comparison

3 Seperate running and cleanup but combine s3 manipulation and snowflake manipulation together to improve efficiency

4 Design user input and options to increase the interaction of users and the program, to avoid them getting lost in the program

5 If in production environment, can split into two program "TovalaAPI_run.py" and "TovalaAPI_cleanup.py", remove the user input steps, just run directly


## 4 Data exploration and analysis based on results comparison from Snowflake
What conclusions can you draw about the keywords you chose and their impact on
visitors to our site?

## 5 Query used in the whole program

Please check the file "Tovala_SQL.txt"
