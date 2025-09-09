package dev.dbos.transact.context;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SetWorkflowIdTests {

    @Test
    @Disabled
    public void SetWorkflowIdTest() {

        // try (SetWorkflowID id1 = new SetWorkflowID("toplevelid1")) {

        //     assertEquals("toplevelid1", DBOSContext.workflowId().get());

        //     try (SetWorkflowID id2 = new SetWorkflowID("innerid1")) {
        //         assertEquals("innerid1", DBOSContext.workflowId().get());

        //         try (SetWorkflowID id3 = new SetWorkflowID("innerid2")) {
        //             assertEquals("innerid2", DBOSContext.workflowId().get());
        //         }

        //         assertEquals("innerid1", DBOSContext.workflowId().get());
        //     }

        //     assertEquals("toplevelid1", DBOSContext.workflowId().get());
        // }
    }
}
