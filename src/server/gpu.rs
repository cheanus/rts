use nvml_wrapper::Nvml;
use std::collections::HashMap;

use super::state::GpuInfo;

/// 通过 NVML 发现所有 GPU。失败返回空 Vec。
pub fn discover_gpus(nvml: &Nvml) -> Vec<GpuInfo> {
    let count = match nvml.device_count() {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    (0..count)
        .filter_map(|i| {
            let device = nvml.device_by_index(i).ok()?;
            let mem = device.memory_info().ok()?;
            Some(GpuInfo {
                index: i,
                total_memory_bytes: mem.total,
            })
        })
        .collect()
}

/// 查询 GPU 池中所有 GPU 的当前空闲显存（字节）。key = GPU index。
/// 失败返回空 HashMap（GPU 任务保持 Pending）。
pub fn query_gpu_free_memory(nvml: &Nvml, gpu_ids: &[u32]) -> HashMap<u32, u64> {
    gpu_ids
        .iter()
        .filter_map(|&id| {
            let device = nvml.device_by_index(id).ok()?;
            let mem = device.memory_info().ok()?;
            Some((id, mem.free))
        })
        .collect()
}
