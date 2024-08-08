package iceberg

import (
	"github.com/apache/iceberg-go/table"
)

type AppendFiles interface {
	appendFile(file DataFile) AppendFiles
	appendManifest(file ManifestFile) AppendFiles
}

type FastAppend struct {
	table.SnapshotProducer
	tableName    string
	ops          table.TableOperations
	spec         PartitionSpec
	newFilePaths Set[string]
	hasNewFiles  bool
	newFiles     []DataFile
}

func NewFastAppend(tableName string, ops table.TableOperations) *FastAppend {
	return &FastAppend{
		tableName:    tableName,
		ops:          ops,
		spec:         ops.Current().PartitionSpec(),
		newFilePaths: make(map[string]bool),
	}
}

func (f *FastAppend) appendFile(file DataFile) AppendFiles {
	Assert(file != nil, "Invalid data file: nil")

	if !f.newFilePaths.Contains(file.FilePath()) {
		f.newFilePaths.Add(file.FilePath())
		f.hasNewFiles = true
		f.newFiles = append(f.newFiles, file)
	}

	return f
}

func (f *FastAppend) appendManifest(manifest ManifestFile) AppendFiles {
	Assert(!manifest.HasExistingFiles(), "Cannot append manifest with existing files")
	Assert(!manifest.HasDeletedFiles(), "Cannot append manifest with deleted files")
	Assert(manifest.SnapshotID() == -1, "Snapshot id must be assigned during commit")
	Assert(manifest.SequenceNum() == -1, "Sequence number must be assigned during commit")

	return f
}

func (f *FastAppend) operation() table.Operation {
	return table.OpAppend
}

func (f *FastAppend) Apply() table.PendingUpdate {

}

type MergeAppend struct {
}

func (m *MergeAppend) appendFile(file DataFile) AppendFiles {
	return m
}

func (m *MergeAppend) appendManifest(manifest ManifestFile) AppendFiles {
	return m
}

func (m *MergeAppend) operation() table.Operation {
	return table.OpAppend
}
