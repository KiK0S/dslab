// VM ALLOCATION EVENTS ////////////////////////////////////////////////////////////////////////////

pub mod allocation {
    use serde::Serialize;

    use crate::resource_pool::Allocation;
    use crate::vm::VirtualMachine;

    #[derive(Serialize)]
    pub struct AllocationRequest {
        pub alloc: Allocation,
        pub vm: VirtualMachine,
    }

    #[derive(Serialize)]
    pub struct MigrationRequest {
        pub source_host: u32,
        pub alloc: Allocation,
        pub vm: VirtualMachine,
    }

    #[derive(Serialize)]
    pub struct AllocationCommitRequest {
        pub alloc: Allocation,
        pub vm: VirtualMachine,
        pub host_id: u32,
    }

    #[derive(Serialize)]
    pub struct AllocationCommitSucceeded {
        pub alloc: Allocation,
        pub host_id: u32,
    }

    #[derive(Serialize)]
    pub struct AllocationCommitFailed {
        pub alloc: Allocation,
        pub host_id: u32,
    }

    #[derive(Serialize)]
    pub struct AllocationFailed {
        pub alloc: Allocation,
        pub host_id: u32,
    }

    #[derive(Serialize)]
    pub struct AllocationReleased {
        pub alloc: Allocation,
        pub host_id: u32,
    }

    #[derive(Serialize, Clone)]
    pub struct AllocationReleaseRequest {
        pub alloc: Allocation,
    }
}

// VM LIFECYCLE EVENTS /////////////////////////////////////////////////////////////////////////////

pub mod vm {
    use serde::Serialize;

    use crate::resource_pool::Allocation;

    #[derive(Serialize)]
    pub struct VMStarted {
        pub alloc: Allocation,
    }

    #[derive(Serialize)]
    pub struct VMDeleted {
        pub alloc: Allocation,
    }
}

// MONITORING EVENTS ///////////////////////////////////////////////////////////////////////////////

pub mod monitoring {
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct HostStateUpdate {
        pub host_id: u32,
        pub cpu_load: f64,
        pub memory_load: f64,
        pub previously_added_vms: Vec<u32>,
        pub previously_removed_vms: Vec<u32>,
    }
}