package nsq

import "testing"

func TestConfigSet(t *testing.T) {
	c := NewConfig()
	if err := c.Set("not a real config value", struct{}{}); err == nil {
		t.Error("No error when setting an invalid value")
	}
	if err := c.Set("verbose", "lol"); err == nil {
		t.Error("No error when setting `verbose` to an invalid value")
	}
	if err := c.Set("verbose", true); err != nil {
		t.Errorf("Error setting `verbose` config: %v", err)
	}
}
