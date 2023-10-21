-- Enable SQL Agent extended stored procs
exec sp_configure 'show advanced options',1
reconfigure
exec sp_configure 'Agent XPs',1
reconfigure

-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db  

-- Then enable CDC for each table to be observed
EXEC sys.sp_cdc_enable_table  
@source_schema = N'dbo',  
@source_name   = N'person',  
@role_name     = null,  
@supports_net_changes = 1  
GO 

-- To disable CDC for a table...
/*
EXEC sys.sp_cdc_disable_table  
@source_schema = N'dbo',  
@source_name   = N'person',
@capture_instance = N'dbo_person' 
GO 
*/