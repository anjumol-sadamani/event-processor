package repository

import (
	"eventprocessor/model"

	"gorm.io/gorm"
)

type SchemaRepository interface {
	InsertSchema(schema model.Schema) error
	InsertSchemaColumn(schemaColumn []model.SchemaColumn) error
}

type SchemaRepositoryImpl struct {
	DB *gorm.DB
}

func CreateSchemaRepository(db *gorm.DB) *SchemaRepositoryImpl {
	return &SchemaRepositoryImpl{DB: db}
}

func (sr *SchemaRepositoryImpl) InsertSchema(schema model.Schema) error {
	return sr.DB.Table("schema").Create(&schema).Error
}

func (sr *SchemaRepositoryImpl) InsertSchemaColumn(schemaColumns []model.SchemaColumn) error {
	return sr.DB.Table("schema_column").Create(&schemaColumns).Error
}
