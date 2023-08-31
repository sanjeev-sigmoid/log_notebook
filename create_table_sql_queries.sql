# Creating Tables
CREATE TABLE DataPipelineRunLog (
    log_id INT IDENTITY(1, 1) PRIMARY KEY,
    user_friendly_name VARCHAR(50),
    datafactory_name VARCHAR(50),
    pipeline_name VARCHAR(50),
    pipelinerunid VARCHAR(50),
    correlation_id VARCHAR(50),
    duration TIME,
    trigger_type VARCHAR(20),
    trigger_id VARCHAR(20),
    trigger_name VARCHAR(50),
    trigger_time DATETIME,
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(20)
);

CREATE TABLE ActivityRunAuditLog (
    log_id INT IDENTITY(1, 1) PRIMARY KEY,
    user_friendly_name VARCHAR(50),
    cluster_name VARCHAR(50),
    duration TIME,
    activity_runid VARCHAR(50),
    pipeline_runid VARCHAR(50),
    pipeline_name VARCHAR(50),
    activity_name VARCHAR(50),
    start_time DATETIME,
    end_time DATETIME,
    source_type VARCHAR(20),
    sink_type VARCHAR(20),
    status VARCHAR(20)
);


# Creating Stored Procedures
CREATE PROCEDURE log_pipeline
    @user_friendly_name VARCHAR(50),
    @datafactory_name VARCHAR(50),
    @pipeline_name VARCHAR(50),
    @pipelinerunid VARCHAR(50),
    @correlation_id VARCHAR(50),
    @duration TIME,
    @trigger_type VARCHAR(20),
    @trigger_id VARCHAR(20),
    @trigger_name VARCHAR(50),
    @trigger_time DATETIME,
    @start_time DATETIME,
    @end_time DATETIME,
    @status VARCHAR(20)
AS
BEGIN
    INSERT INTO [dbo].[DataPipelineRunLog] (
        user_friendly_name,
        datafactory_name,
        pipeline_name,
        pipelinerunid,
        correlation_id,
        duration,
        trigger_type,
        trigger_id,
        trigger_name,
        trigger_time,
        start_time,
        end_time,
        status
    ) VALUES (
        @user_friendly_name,
        @datafactory_name,
        @pipeline_name,
        @pipelinerunid,
        @correlation_id,
        @duration,
        @trigger_type,
        @trigger_id,
        @trigger_name,
        @trigger_time,
        @start_time,
        @end_time,
        @status
    );
END;

CREATE PROCEDURE [dbo].[log_pipeline_activity]
    @user_friendly_name VARCHAR(50),
    @cluster_name VARCHAR(50),
    @duration TIME,
    @activity_runid VARCHAR(50),
    @pipeline_runid VARCHAR(50),
    @pipeline_name VARCHAR(50),
    @activity_name VARCHAR(50),
    @start_time DATETIME,
    @end_time DATETIME,
    @source_type VARCHAR(20),
    @sink_type VARCHAR(20),
    @status VARCHAR(20)
AS
BEGIN
    INSERT INTO [dbo].[ActivityRunAuditLog] (
        user_friendly_name,
        cluster_name,
        duration,
        activity_runid,
        pipeline_runid,
        pipeline_name,
        activity_name,
        start_time,
        end_time,
        source_type,
        sink_type,
        status
    )
    VALUES (
        @user_friendly_name,
        @cluster_name,
        @duration,
        @activity_runid,
        @pipeline_runid,
        @pipeline_name,
        @activity_name,
        @start_time,
        @end_time,
        @source_type,
        @sink_type,
        @status
    )
END;
