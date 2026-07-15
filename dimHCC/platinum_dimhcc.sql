CREATE TABLE [devidap1].[platinum_dimhcc]
(
 [HCCNumber] nvarchar(20)
,[HCCDescription] nvarchar(512)
,[HCCVersion] nvarchar(20)
,[HCCType] nvarchar(10)
,[IsChronic] bit
,[EffectiveYear] int
,[EffectiveDateStart] date
,[EffectiveDateEnd] date
,[hashKey] int
,[hccKey] int NOT NULL PRIMARY KEY NONCLUSTERED ([hccKey] ASC) NOT ENFORCED

) 