ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "%1$s"."notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "%1$s"."workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "%1$s"."workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "%1$s"."operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "%1$s"."streams" ADD COLUMN "serialization" TEXT DEFAULT NULL;