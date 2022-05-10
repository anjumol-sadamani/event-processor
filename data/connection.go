package data

import (
	"eventprocessor/config"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func Connection() (*gorm.DB, error) {

	config := config.GetConfig()

	DBurl := fmt.Sprintf("host=%s port=%s user=%s password= %s dbname=%s  sslmode = disable",
		config.DBHost,
		config.DBPort,
		config.DBUserName,
		config.DBPassword,
		config.DBName,
	)

	db, err := gorm.Open(postgres.Open(DBurl), &gorm.Config{})
	if err != nil {
	log.Errorf("Error in Connecting to DB  %v", err)
		return nil, err
	}
	log.Info("DB Connection successfull")
	log.Info("Running migrations")
	runDatabaseMigrations(config)
	log.Info("Migration successfull")

	return db, nil
}

func runDatabaseMigrations(config *config.Config) {
	m, err := migrate.New(
		"file://data/migrations",
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", config.DBUserName, config.DBPassword, config.DBHost, config.DBPort, config.DBName))
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatal(err)
	}
}
