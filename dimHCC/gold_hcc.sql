CREATE TABLE [devidap1].[gold_hcc]
( 
	 [HCCNumber] nvarchar(20) NOT NULL PRIMARY KEY NONCLUSTERED ([HCCNumber] ASC,[HCCVersion] ASC,[HCCType] ASC,[EffectiveYear] ASC) NOT ENFORCED
	,[HCCDescription] nvarchar(512)
	,[HCCVersion] nvarchar(20) NOT NULL
	,[HCCType] nvarchar(10) NOT NULL
	,[IsChronic] bit
	,[EffectiveYear] int NOT NULL
	,[EffectiveDateStart] date
	,[EffectiveDateEnd] date
	,[hashKey] int
) 
