-- Insert data using INSERT INTO
INSERT INTO DataPipelineRunLog (
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
    'Uttam',
    'MyDataFactory',
    'MyPipeline',
    '12345',
    'abc123',
    '00:10:00',
    'Manual',
    'trigger1',
    'Trigger 1',
    '2023-07-21 12:34:56',
    '2023-07-21 12:00:00',
    '2023-07-21 12:10:00',
    'Success'
);

INSERT INTO ActivityRunAuditLog (
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
) VALUES (
    'Uttam',
    'MyCluster',
    '00:05:00',
    'activity123',
    '12345',
    'MyPipeline',
    'MyActivity',
    '2023-07-21 12:00:00',
    '2023-07-21 12:05:00',
    'Source',
    'Sink',
    'Success'
);

-- Insert data using stored procedures
-- Example 1
EXEC [dbo].[log_pipeline] 'John', 'DataFactory1', 'PipelineA', 'RunID123', 'corr123', '00:15:00', 'Scheduled', 'trigger2', 'Trigger 2', '2023-07-21 13:45:00', '2023-07-21 13:00:00', '2023-07-21 13:15:00', 'Success';

-- Example 2
EXEC [dbo].[log_pipeline] 'Jane', 'DataFactory2', 'PipelineB', 'RunID456', 'corr456', '00:20:00', 'Manual', 'trigger3', 'Trigger 3', '2023-07-21 14:30:00', '2023-07-21 14:10:00', '2023-07-21 14:30:00', 'Failed';


-- Example 1
EXEC [dbo].[log_pipeline_activity] 'John', 'Cluster1', '00:03:30', 'activity123a', 'RunID123', 'PipelineA', 'ActivityX', '2023-07-21 13:00:00', '2023-07-21 13:03:30', 'Source', 'Sink', 'Success';

-- Example 2
EXEC [dbo].[log_pipeline_activity] 'Jane', 'Cluster2', '00:05:45', 'activity456b', 'RunID456', 'PipelineB', 'ActivityY', '2023-07-21 14:10:00', '2023-07-21 14:15:45', 'Source', 'Sink', 'Failed';
