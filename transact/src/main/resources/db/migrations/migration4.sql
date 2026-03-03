ALTER TABLE "%1$s".workflow_status ADD COLUMN forked_from TEXT;
CREATE INDEX "idx_workflow_status_forked_from" ON "%1$s"."workflow_status" ("forked_from");