CREATE TABLE [devidap1].[platinum_dimalertgroup]
( 
  [alertGroupKey] int NOT NULL PRIMARY KEY NONCLUSTERED ([alertGroupKey] ASC) NOT ENFORCED
 ,[alertGroupCode] nvarchar(20)
 ,[alertGroupDescription] varchar(512)
 ,[displayText] varchar(512)
 ,[sortOrder] int
 ,[isActive] bit
) 