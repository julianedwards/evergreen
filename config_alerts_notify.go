package evergreen

import (
	"github.com/evergreen-ci/evergreen/db"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
)

// NotifyConfig hold logging and email settings for the notify package.
type NotifyConfig struct {
	BufferTargetPerInterval int         `bson:"buffer_target_per_interval" json:"buffer_target_per_interval" yaml:"buffer_target_per_interval"`
	BufferIntervalSeconds   int         `bson:"buffer_interval_seconds" json:"buffer_interval_seconds" yaml:"buffer_interval_seconds"`
	SMTP                    *SMTPConfig `bson:"smtp" json:"smtp" yaml:"smtp"`
}

func (c *NotifyConfig) SectionId() string { return "notify" }

func (c *NotifyConfig) Get() error {
	err := db.FindOneQ(ConfigCollection, db.Query(byId(c.SectionId())), c)
	if err != nil && err.Error() == errNotFound {
		*c = NotifyConfig{}
		return nil
	}
	return errors.Wrapf(err, "error retrieving section %s", c.SectionId())
}

func (c *NotifyConfig) Set() error {
	_, err := db.Upsert(ConfigCollection, byId(c.SectionId()), c)
	return errors.Wrapf(err, "error updating section %s", c.SectionId())
}

func (c *NotifyConfig) ValidateAndDefault() error {
	if c.BufferIntervalSeconds <= 0 {
		c.BufferIntervalSeconds = 60
	}
	if c.BufferTargetPerInterval <= 0 {
		c.BufferTargetPerInterval = 20
	}

	// cap to 100 jobs/sec per server
	jobsPerSecond := c.BufferIntervalSeconds / c.BufferTargetPerInterval
	if jobsPerSecond > maxNotificationsPerSecond {
		return errors.Errorf("maximum notification jobs per second is %d", maxNotificationsPerSecond)

	}

	return nil
}

type AlertsConfig struct {
	SMTP *SMTPConfig `bson:"smtp" json:"smtp" yaml:"smtp"`
}

func (c *AlertsConfig) SectionId() string { return "alerts" }

func (c *AlertsConfig) Get() error {
	err := db.FindOneQ(ConfigCollection, db.Query(byId(c.SectionId())), c)
	if err != nil && err.Error() == errNotFound {
		*c = AlertsConfig{}
		return nil
	}
	return errors.Wrapf(err, "error retrieving section %s", c.SectionId())
}

func (c *AlertsConfig) Set() error {
	_, err := db.Upsert(ConfigCollection, byId(c.SectionId()), bson.M{
		"$set": bson.M{
			"smtp": c.SMTP,
		},
	})
	return errors.Wrapf(err, "error updating section %s", c.SectionId())
}

func (c *AlertsConfig) ValidateAndDefault() error { return nil }

// SMTPConfig holds SMTP email settings.
type SMTPConfig struct {
	Server     string   `bson:"server" json:"server" yaml:"server"`
	Port       int      `bson:"port" json:"port" yaml:"port"`
	UseSSL     bool     `bson:"use_ssl" json:"use_ssl" yaml:"use_ssl"`
	Username   string   `bson:"username" json:"username" yaml:"username"`
	Password   string   `bson:"password" json:"password" yaml:"password"`
	From       string   `bson:"from" json:"from" yaml:"from"`
	AdminEmail []string `bson:"admin_email" json:"admin_email" yaml:"admin_email"`
}
