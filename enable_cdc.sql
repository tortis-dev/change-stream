EXEC sys.sp_cdc_enable_db  

EXEC sys.sp_cdc_enable_table  
@source_schema = N'dbo',  
@source_name   = N'person',  
@role_name     = null,  
@supports_net_changes = 1  
GO 

EXEC sys.sp_cdc_disable_table  
@source_schema = N'dbo',  
@source_name   = N'person',
@capture_instance = N'dbo_person' 
GO 
