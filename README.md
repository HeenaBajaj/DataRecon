# DataRecon
Data Reconciliation
‘Data Recon’ is a project to automate the data reconciliations using Python(to prepare back-end) and PyQT(to design front-end). Our project provides a user friendly front-end to our analysts who can perform various operations at ease. The current scope of the project lies at our DA team of Actuarial and we plan to extend the scope to IT team to help them automate the reconciliations made at their end.

Improved speed:
- Create to two servers at one instance and compare the data in python. This helps in creating the recons at a very high speed as compared to manually pasting the data.
- No manual effort to query the data and compare in excel by creating pivots
		
Less error prone: 
- Manual files can be error prone whereas automated files have less scope of any errors. 
	
Automated Debug Checks:
- When a recon is created the differences are debugged manually. This is the main highlight of our project that user will be given a pre-debugged reconciliation where he would get the data fields which have differences. For example, say we have 5 underwriting years, 2014,2015,2016,2018 and 2019 and there is no difference in 2014 and 2015.  Along with a total reconciliation which will have all the years, the user also gets a debugged file which will have 2016,2018 and 2019 only with the difference amounts from the two tables so that he can directly work on resolving the differences. The effort he spends collating the values is saved. This can happen on as many fields as the user requires. 
