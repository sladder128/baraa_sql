SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		DECLARE @start_time datetime, @end_time datetime, @layer_end_time datetime, @layer_start_time datetime;
		
		SET @start_time = getdate();
		SET @layer_start_time= getdate();
		Truncate table bronze.crm_cust_info;
		Bulk insert bronze.crm_cust_info from 'C:\Users\bol5e\Downloads\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT 'crm_cust_info tqble loading duration is ' + cast(Datediff(second, @start_time, @end_time) as nvarchar);

		Truncate table bronze.crm_prd_info;
		Bulk insert bronze.crm_prd_info from 'C:\Users\bol5e\Downloads\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);


		Truncate table bronze.crm_sales_details;
		Bulk insert bronze.crm_sales_details from 'C:\Users\bol5e\Downloads\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);


		Print '==================NOW LOAD ERP===========================';

		Truncate table bronze.erp_cust_az12;
		Bulk insert bronze.erp_cust_az12 from 'C:\Users\bol5e\Downloads\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		WITH (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);


		Truncate table bronze.erp_loc_a101;
		Bulk insert bronze.erp_loc_a101 from 'C:\Users\bol5e\Downloads\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		WITH (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);


		Truncate table bronze.erp_px_cat_g1v2;
		Bulk insert bronze.erp_px_cat_g1v2 from 'C:\Users\bol5e\Downloads\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
	END TRY
	BEGIN CATCH
		PRINT 'Bronze loading failed'
	END CATCH
	SET @layer_end_time= getdate();
	Print '=============================================';
	PRINT 'bronze layer loading duration is ' + cast(Datediff(second, @layer_start_time, @layer_end_time) as nvarchar) + ' seconds'
END
