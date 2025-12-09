CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
BEGIN TRY
		DECLARE @start_dt DATETIME , @end_dt DATETIME
		PRINT '========================================';
		PRINT 'Loading DataSet';
		PRINT '========================================';

		PRINT '----------------------------------------';
		PRINT 'Loading CRM DataSet';
		PRINT '----------------------------------------';

		SET @start_dt = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting Table : bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\SQL_Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_dt = GETDATE();
			
		SELECT COUNT(*) FROM bronze.crm_cust_info

		PRINT '>> Loading time: ' + CAST(DATEDIFF(second, @start_dt, @end_dt) AS NVARCHAR) + ' seconds';

		PRINT '>> Truncating Table : bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Table : bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\SQL_Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.crm_prd_info

		PRINT '>> Truncating Table : bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Table : bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\SQL_Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.crm_sales_details

		PRINT '----------------------------------------';
		PRINT 'Loading ERP DataSet';
		PRINT '----------------------------------------';

		PRINT '>> Truncating Table : bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting Table : bronze.erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\SQL_Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_cust_az12

		PRINT '>> Truncating Table : bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting Table : bronze.erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\SQL_Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_loc_a101

		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Table : bronze.erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\SQL_Course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2
END TRY
BEGIN CATCH
		PRINT '========================================';
		PRINT 'Error in Loading Bronze DataSet';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '========================================';
END CATCH
END
