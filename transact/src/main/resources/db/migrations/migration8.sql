ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "%1$s"."workflow_status" ("parent_workflow_id");