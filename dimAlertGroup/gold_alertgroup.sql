CREATE TABLE [devidap1].[gold_alertgroup]
( 
	 [alertGroupID] int NOT NULL PRIMARY KEY NONCLUSTERED ([alertGroupID] ASC) NOT ENFORCED
	,[alertGroupCode] nvarchar(20)
	,[alertGroupDescription] nvarchar(512)
	,[displayText] nvarchar(512)
	,[sortOrder] int
	,[isActive] bit
	,[hashKey] int
) 