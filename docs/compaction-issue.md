# 问题概述
- 写入基准（如插入 1 亿条）中，压实阶段一次性把所有输入 SST 全部读入内存再排序（`src/db.cc:812-931` 调用 `SSTable::LoadAll`），大 L0 文件导致磁盘 IO 掉到 0、CPU 满载，暂停时间长。
- Memtable 刷盘忽略 `sstable_target_size_bytes`，整块写成单个 L0 文件（`src/db.cc:734-780`）；在 512MB memtable 设置下，每个 L0 文件很大，压实输入集巨大。
- 压实还多次复制数据（`merged_entries`→`compacted`→分块输出），峰值内存进一步放大；LevelDB 使用流式归并避免这种膨胀。
- 若存在快照/迭代器，`BuildMergedView` 会把所有 SST 加载进 map，进一步抬高内存，虽然纯写基准通常不触发。

# 现象
- 压实期间 IO 暂停、CPU 满载。
- 内存使用暴涨（约 10GB，对比 LevelDB 同参数约 1GB）。

# 建议修复
- 将压实改为流式 k-way merge（迭代器 + 小顶堆，边读边写），避免全量加载 + 排序。
- 刷盘时按 `sstable_target_size_bytes` 切分 memtable 输出成多个 L0 文件，或降低 `memtable_soft_limit_bytes` 作为权宜缓解。
- 减少压实中的额外拷贝，直接从流式 merge 输出分块写文件。
- 记录/避免重写场景下长期快照造成的内存放大。
