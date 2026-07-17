# 持久化状态备份与恢复

## 适用范围

DAGForge 的文件存储是单写者状态目录，包含 Plan catalog、Run checkpoint、Evidence 日志、Artifact 数据与元数据以及 `.dagforge.lock`。有效备份必须在同一个静止时间点完整捕获整个目录，不得把不同备份中的子目录或单个文件拼接到一起。

当前预发布版本只接受显式 version 1 存储 envelope。无版本的开发期状态和不支持的未来版本都会 fail closed。每份备份必须同时保存创建它的 DAGForge release archive 和 System Configuration。

## 停止服务并确认所有权

先正常停止 DAGForge 并等待进程退出，再确认状态目录锁可以获取。以下示例中的 `/var/lib/dagforge/state` 应替换为 `storage.directory` 的实际值。

```bash
state=/var/lib/dagforge/state
exec 9<>"$state/.dagforge.lock"
flock -n 9 || { echo "DAGForge 仍持有状态目录" >&2; exit 1; }
```

不得直接复制正在使用的状态目录。DAGForge 的 advisory lock 可以阻止另一个 Application 打开同一目录，但普通备份工具不会自动参与该锁协议。

## 创建备份

备份必须保留完整目录树、权限、所有权、稀疏文件和时间戳。以下示例创建压缩归档、配置副本和校验和。

```bash
state=/var/lib/dagforge/state
backup=/var/backups/dagforge
stamp=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup"
tar --numeric-owner --acls --xattrs --sparse -C "$(dirname "$state")" -czf "$backup/state-$stamp.tar.gz" "$(basename "$state")"
install -m 0600 /etc/dagforge/system_config.json "$backup/system_config-$stamp.json"
sha256sum "$backup/state-$stamp.tar.gz" "$backup/system_config-$stamp.json" > "$backup/sha256sums-$stamp.txt"
```

归档旁应记录 DAGForge release version、Minijail revision、主机架构、文件系统类型和备份命令输出。备份必须存放在状态文件系统之外，并使用与 Workflow 输入和输出相同的保密控制。

## 验证备份

解压前先验证 checksum。归档应解压到隔离的 staging 目录，只允许一个预期状态根目录，不得包含 symlink 或 group/world writable 路径。

```bash
backup=/var/backups/dagforge
stamp=YYYYMMDDTHHMMSSZ
stage=$(mktemp -d)
cd "$backup"
sha256sum -c "sha256sums-$stamp.txt"
tar --no-same-owner --no-overwrite-dir -xzf "state-$stamp.tar.gz" -C "$stage"
find "$stage" -type l -print -quit | grep -q . && { echo "备份包含符号链接" >&2; exit 1; }
find "$stage" -perm /022 -print -quit | grep -q . && { echo "备份包含组或全局可写状态" >&2; exit 1; }
```

完整恢复演练应使用解压状态的副本，在隔离端口启动创建该备份的同一 DAGForge release，确认启动恢复与 Artifact reconciliation 成功，并执行只读 Plan、Run 和 Artifact 查询。启动过程可能修复 Evidence 最后一段不完整记录或执行 retention compaction，因此不得直接拿唯一备份副本做演练。

## 恢复

停止 DAGForge，把当前状态目录整体移动为一个 rollback unit，在空父目录中解压备份，然后恢复服务账户所有权和限制性权限。`.dagforge.lock` 中可能保留旧 PID 文本，这不影响恢复，因为真正的所有权由实时 advisory lock 决定，成功获取锁后 PID 会被覆盖。

```bash
state=/var/lib/dagforge/state
backup=/var/backups/dagforge/state-YYYYMMDDTHHMMSSZ.tar.gz
service_user=dagforge
service_group=dagforge
mv "$state" "$state.before-restore"
mkdir -p "$(dirname "$state")"
tar --numeric-owner --acls --xattrs --sparse -C "$(dirname "$state")" -xzf "$backup"
chown -R "$service_user:$service_group" "$state"
chmod 0700 "$state"
chmod 0600 "$state/.dagforge.lock"
```

使用匹配的配置启动 DAGForge。启动必须成功获取 `.dagforge.lock`、验证所有 managed catalog entry、打开 Evidence、reconcile Artifact pair、先恢复 Plan 再恢复 Checkpoint，并在损坏、未知存储版本、不安全权限或超限文件上 fail closed。任何启动错误都应视为恢复失败，在 Workflow 和 Artifact 检查通过前不得删除 rollback 目录。

## 回滚

停止恢复失败的实例，把恢复后的目录移走，再将 `state.before-restore` 重命名回配置路径。不得把失败恢复目录与旧状态合并；需要保留两份状态调查时，应将它们设置为只读并移出 active path。

## 保留与演练

应保留多个带日期的备份并定期在隔离主机执行恢复演练。归档命令成功不代表可恢复；验收标准是匹配二进制能够干净启动、Plan 与 Run inventory 符合预期、保留的 Artifact 可读取且没有无法解释的 reconciliation debt。
