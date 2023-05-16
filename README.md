# tracker_changes_SQL

var trackLogger = new TrackerTriggerSQLServer("DBContext");
trackLogger.AddTriggers("Project.Model.Entities");
