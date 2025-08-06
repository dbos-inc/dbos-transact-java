package dev.dbos.transact.context;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SetWorkflowIdTests {

    @Test
    public void SetWorkflowIdTest() {

        try (SetWorkflowID id1 = new SetWorkflowID("toplevelid1")) {

            assertEquals("toplevelid1",DBOSContextHolder.get().getWorkflowId());

            try (SetWorkflowID id2 = new SetWorkflowID("innerid1")) {
                assertEquals("innerid1",DBOSContextHolder.get().getWorkflowId());

                try (SetWorkflowID id3 = new SetWorkflowID("innerid2")) {
                    assertEquals("innerid2",DBOSContextHolder.get().getWorkflowId());
                }

                assertEquals("innerid1",DBOSContextHolder.get().getWorkflowId());
            }

            assertEquals("toplevelid1",DBOSContextHolder.get().getWorkflowId());
        }
    }
}
