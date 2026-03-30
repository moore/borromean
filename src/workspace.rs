//= spec/implementation.md#memory-requirements
//# `RING-IMPL-MEM-002` Any operation that needs scratch space for encoding, decoding, or staging MUST accept caller-provided buffers or borrow dedicated storage from a caller-provided workspace object.
pub struct StorageWorkspace<const REGION_SIZE: usize> {
    region_bytes: [u8; REGION_SIZE],
    physical_scratch: [u8; REGION_SIZE],
    logical_scratch: [u8; REGION_SIZE],
}

impl<const REGION_SIZE: usize> StorageWorkspace<REGION_SIZE> {
    pub fn new() -> Self {
        Self {
            region_bytes: [0u8; REGION_SIZE],
            physical_scratch: [0u8; REGION_SIZE],
            logical_scratch: [0u8; REGION_SIZE],
        }
    }

    pub(crate) fn scan_buffers(&mut self) -> (&mut [u8; REGION_SIZE], &mut [u8; REGION_SIZE]) {
        (&mut self.region_bytes, &mut self.logical_scratch)
    }

    //= spec/implementation.md#memory-requirements
    //# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk format constants MUST be derivable from public constants, associated constants, or documented constructor contracts.
    pub(crate) fn encode_buffers(&mut self) -> (&mut [u8; REGION_SIZE], &mut [u8; REGION_SIZE]) {
        (&mut self.physical_scratch, &mut self.logical_scratch)
    }
}

impl<const REGION_SIZE: usize> Default for StorageWorkspace<REGION_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}
