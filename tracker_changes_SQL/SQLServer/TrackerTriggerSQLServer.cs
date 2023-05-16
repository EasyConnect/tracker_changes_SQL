using System.ComponentModel.DataAnnotations.Schema;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using tracker_changes_SQL.Attributes;
using tracker_changes_SQL.Common;

namespace tracker_changes_SQL.SQLServer
{
    public class TrackerTriggerSQLServer : ITrackerTrigger
    {
        public readonly string ConnectionString;
        private const string TableLogName = "TrackLog";

        public TrackerTriggerSQLServer(string NameConnectionString)
        {
            ConnectionString = ConfigurationManager.ConnectionStrings[NameConnectionString].ConnectionString;
            ExecuteNonQuery(GetCreateTable(), ConnectionString);
        }

        private const string SQL_CREATE_TABLE_LOG = @"
                IF OBJECT_ID('{0}') IS NULL
					CREATE TABLE {0} (
						 [ID] [uniqueidentifier] NOT NULL PRIMARY KEY DEFAULT NEWSEQUENTIALID()
						,[Type] [char](1) NULL
						,[TableSchema] [nchar](15) NULL
						,[TableName] [varchar](128) NULL
						,[PK] [varchar](MAX) NULL
						,[PKFieldName] [varchar](MAX) NULL
						,[FieldName] [varchar](MAX) NULL
						,[OldValue] [varchar](MAX) NULL
						,[NewValue] [varchar](MAX) NULL
						,[DateHour] [datetime] NULL
						,[Application] [nvarchar](150) NULL
						,[EntityId] INT NOT NULL
						)";

        private string GetCreateTable() => string.Format(SQL_CREATE_TABLE_LOG, TableLogName);

        private const string SQL_CREATE_TRIGGER = @"
                CREATE OR ALTER TRIGGER {0}_logger ON {0}
				FOR INSERT
					,UPDATE
					,DELETE
				AS
				DECLARE
					 @bit INT
					,@field INT
					,@fieldId INT
					,@maxfield INT
					,@char INT
					,@fieldName VARCHAR(MAX)
					,@fieldNameAux VARCHAR(MAX) = ''
					,@TableName VARCHAR(128)
					,@SchemaName VARCHAR(128)
					,@PKCols VARCHAR(MAX)
					,@PKFieldName VARCHAR(MAX)
					,@PKSelect VARCHAR(MAX)
					,@sql VARCHAR(MAX)
					,@DateHour DATETIME
					,@Type CHAR(1)
					,@FullTableName VARCHAR(256)
					,@next BIT
					,@oldValue VARCHAR(MAX)
					,@newValue VARCHAR(MAX)
					,@newValueAux VARCHAR(MAX) = ''

				SELECT @TableName = '{0}'
					  ,@schemaName = '{1}'

				----------------------------------------------------------------------------------------------
				SELECT @FullTableName = '[' + @SchemaName + '].[' + @TableName + ']'

				-- selecting the action Insert, delete or update
				SET @Type = (
						CASE 
							WHEN EXISTS (
									SELECT *
									FROM INSERTED
									)
								AND EXISTS (
									SELECT *
									FROM DELETED
									)
								THEN 'U' -- Set Action to Updated.
							WHEN EXISTS (
									SELECT *
									FROM INSERTED
									)
								THEN 'I' -- Set Action to Insert.
							WHEN EXISTS (
									SELECT *
									FROM DELETED
									)
								THEN 'D' -- Set Action to Deleted.
							ELSE NULL -- Skip. It may have been a ""failed delete"".   
							END
						)


				-- get list of columns
				SELECT *
				INTO #ins
				FROM inserted

				SELECT *
				INTO #del
				FROM deleted

				SELECT @PKFieldName = COALESCE(@PKFieldName+ ',' + C.COLUMN_NAME, C.COLUMN_NAME)
				FROM INFORMATION_SCHEMA.COLUMNS C
				JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CS ON CS.TABLE_NAME = C.TABLE_NAME
					AND CS.COLUMN_NAME = C.COLUMN_NAME
				JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON PK.TABLE_NAME = CS.TABLE_NAME
					AND PK.CONSTRAINT_NAME = CS.CONSTRAINT_NAME
				WHERE PK.TABLE_NAME = @TableName
					  AND PK.CONSTRAINT_TYPE = 'PRIMARY KEY'

				SELECT @PKCols = COALESCE(@PKCols + ' and', ' on') + ' i.' + C.COLUMN_NAME + ' = d.' + C.COLUMN_NAME
				FROM INFORMATION_SCHEMA.COLUMNS C
				JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CS ON CS.TABLE_NAME = C.TABLE_NAME
					AND CS.COLUMN_NAME = C.COLUMN_NAME
				JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON PK.TABLE_NAME = CS.TABLE_NAME
					AND PK.CONSTRAINT_NAME = CS.CONSTRAINT_NAME
				WHERE PK.TABLE_NAME = @TableName
					  AND PK.CONSTRAINT_TYPE = 'PRIMARY KEY'

				SELECT @PKSelect = COALESCE(@PKSelect + '+', '') + '''' + '''+convert(varchar(100),coalesce(i.' + C.COLUMN_NAME + ',d.' + C.COLUMN_NAME + '))+'','''
				FROM INFORMATION_SCHEMA.COLUMNS C
				JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CS ON CS.TABLE_NAME = C.TABLE_NAME
					AND CS.COLUMN_NAME = C.COLUMN_NAME
				JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON PK.TABLE_NAME = CS.TABLE_NAME
					AND PK.CONSTRAINT_NAME = CS.CONSTRAINT_NAME
				WHERE PK.TABLE_NAME = @TableName
					  AND PK.CONSTRAINT_TYPE = 'PRIMARY KEY'

				-- error handling
				IF @PKCols IS NULL
				BEGIN
					RAISERROR (
							'no PK on table %s'
							,16
							,- 1
							,@TableName
							)
					RETURN
				END

				-- initiating variables
				SELECT @field = MIN(C.ORDINAL_POSITION), @maxfield = MAX(C.ORDINAL_POSITION)
				FROM INFORMATION_SCHEMA.COLUMNS C
				LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CS ON CS.TABLE_NAME = C.TABLE_NAME AND CS.COLUMN_NAME = C.COLUMN_NAME
				LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON PK.TABLE_NAME = CS.TABLE_NAME AND PK.CONSTRAINT_NAME = CS.CONSTRAINT_NAME
				WHERE C.TABLE_NAME = @TableName
					AND C.TABLE_SCHEMA = @SchemaName
					AND (
						(
							@type = 'D'
							AND PK.CONSTRAINT_TYPE = 'PRIMARY KEY'
							)
						OR @type != 'D'
						)

				--- main while loop ---
				WHILE @field < @maxfield + 1
				BEGIN
					DECLARE @SelectFK VARCHAR(MAX) = NULL

					-- fetching the field names for each column
					SELECT @fieldName = COLUMN_NAME
					FROM INFORMATION_SCHEMA.COLUMNS
					WHERE TABLE_NAME = @TableName
						AND ORDINAL_POSITION = @field
						AND TABLE_SCHEMA = @SchemaName

					-- fetching the column ID, these can be different from the ordinal position if the columns are dropped 
					SELECT @fieldid = COLUMNPROPERTY(OBJECT_ID(@FullTableName), @fieldName, 'ColumnID')

					-- fetching the ordinal position   
					SELECT @field = MIN(ORDINAL_POSITION)
					FROM INFORMATION_SCHEMA.COLUMNS
					WHERE TABLE_NAME = @TableName
						AND TABLE_SCHEMA = @SchemaName
						AND ORDINAL_POSITION > @field

					-- capturing some values to do the condition for the updated action
					SELECT @bit = (@fieldId - 1) % 8 + 1
					SELECT @bit = POWER(2, @bit - 1)
					SELECT @char = ((@fieldId - 1) / 8) + 1
					SELECT @next = 1
				
					-------------------------------------------------------------------------------------------------------------
					IF @next = 1
					BEGIN
						SELECT @oldValue = 'd.' + @fieldName
						SELECT @newValue = 'i.' + @fieldName
						-- Skip apostrophe in the updated values
						IF CHARINDEX('''', @newValue) > 0
							SET @newValue = REPLACE(@newValue, '''', '''''')
						IF CHARINDEX('''', @oldValue) > 0
							SET @oldValue = REPLACE(@oldValue, '''', '''''')

					IF  @Type IN ('I')
					BEGIN	
						IF(@fieldName IS NOT NULL)
							SET @fieldNameAux = @fieldNameAux + @fieldName + ',';

						IF(@newValue IS NOT NULL)
							SET @newValueAux = @newValueAux  + ' IIF(' + @newValue + ' IS NULL, ''NULL'','''''''' + CONVERT(VARCHAR(MAX),' + @newValue + ') + '''''''') + '','' +';
					END
					
					----------------------------------------------------------------------------
					-- Select COLUMNS_UPDATED(),@char, @bit, @Type						
					-- The first condition checks for updates and next one for deletes
					IF (SUBSTRING(COLUMNS_UPDATED(), @char, 1) & @bit > 0 AND @Type IN ('U')) OR (@field IS NULL AND @Type IN ('I')) OR @Type IN ('D')
					BEGIN
						SELECT @sql = '
							INSERT {2}
							(Type, TableSchema, TableName, PK, PKFieldName, FieldName, OldValue, NewValue, DateHour, Application, EntityId)   
							SELECT ' + IIF(@Type = 'I', 'TOP 1 ', '') + '''' + @Type + ''',''' + @SchemaName + ''',
							''' + @TableName + ''',' + SUBSTRING(@PKSelect, 1, LEN(@PKSelect) - 4) + ',''' + @PKFieldName + ''',''' + IIF(@Type = 'I', SUBSTRING(@fieldNameAux, 1, LEN(@fieldNameAux) - 1), @fieldName) + '''' + ', ' + IIF(@Type = 'I', 'NULL', @oldValue) + ',
							' + IIF(@Type = 'I', '(SELECT ' + SUBSTRING(@newValueAux, 1, LEN(@newValueAux) - 8) + ')', @newValue) + ', GETUTCDATE(),
							''' + REPLACE(APP_NAME(), '''', '''''') + '''' + ', 0 
							FROM #ins i FULL OUTER JOIN #del d' + @PKCols + ' 
							WHERE i.' + @fieldName + ' <> d.' + @fieldName + ' OR (i.' + @fieldName + ' IS NULL AND  d.' + @fieldName + ' IS NOT NULL)' + ' OR (i.' + @fieldName + ' IS NOT NULL AND  d.' + @fieldName + ' IS NULL)'
							--ORDER BY i.'+ REPLACE(@PKFieldName, ',',',i.')+',d.'+ REPLACE(@PKFieldName, ',',',d.')
							
						--PRINT @sql

						EXEC (@sql) -- excuting the sql into the GenericDataTracking table
					END 
				END
			END -- while loop end";

        private string GetCreateTrigger(string tableName, string? schemaName) =>
            string.Format(SQL_CREATE_TRIGGER, tableName, string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName, TableLogName);

        private static void ExecuteNonQuery(string commandText, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                conn.Open();
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        private void CreateTrigger(string tableName, string schemaName)
        {
            ExecuteNonQuery(GetCreateTrigger(tableName, schemaName), ConnectionString);
        }

        public void AddTriggers(string namespaces)
        {
            var types = AppDomain.CurrentDomain.GetAssemblies()
                          .SelectMany(t => t.GetTypes())
                          .Where(t => t.IsClass && t.Namespace == namespaces);

            foreach (var type in types)
            {
                var IsTrack = AttributeTool.GetAttribute<TrackAttribute>(type) != null;

                if (IsTrack)
                {
                    var TableAttribute = AttributeTool.GetAttribute<TableAttribute>(type);

                    if (TableAttribute != null)
                        CreateTrigger(TableAttribute.Name, TableAttribute.Schema);
                }
            }
        }
    }
}
