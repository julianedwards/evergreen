package route

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/evergreen-ci/evergreen"

	"github.com/evergreen-ci/evergreen/db"
	"github.com/evergreen-ci/evergreen/model/event"
	"github.com/evergreen-ci/evergreen/model/user"
	"github.com/evergreen-ci/evergreen/rest/data"
	restModel "github.com/evergreen-ci/evergreen/rest/model"
	"github.com/evergreen-ci/evergreen/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type AdminRouteSuite struct {
	sc data.Connector
	suite.Suite
	getHandler  MethodHandler
	postHandler MethodHandler
}

func TestAdminRouteSuiteWithDB(t *testing.T) {
	s := new(AdminRouteSuite)
	db.SetGlobalSessionProvider(testutil.TestConfig().SessionFactory())
	s.sc = &data.DBConnector{}
	testutil.HandleTestingErr(db.ClearCollections(evergreen.ConfigCollection), t,
		"Error clearing collections")

	// run the rest of the tests
	suite.Run(t, s)
}

func TestAdminRouteSuiteWithMock(t *testing.T) {
	s := new(AdminRouteSuite)
	s.sc = &data.MockConnector{}

	// run the rest of the tests
	suite.Run(t, s)
}

func (s *AdminRouteSuite) SetupSuite() {
	// test getting the route handler
	const route = "/admin/settings"
	const version = 2
	routeManager := getAdminSettingsManager(route, version)
	s.NotNil(routeManager)
	s.Equal(route, routeManager.Route)
	s.Equal(version, routeManager.Version)
	s.getHandler = routeManager.Methods[0]
	s.postHandler = routeManager.Methods[1]
	s.IsType(&adminGetHandler{}, s.getHandler.RequestHandler)
	s.IsType(&adminPostHandler{}, s.postHandler.RequestHandler)
}

func (s *AdminRouteSuite) TestAdminRoute() {
	ctx := context.Background()
	ctx = context.WithValue(ctx, evergreen.RequestUser, &user.DBUser{Id: "user"})
	testSettings := testutil.MockConfig()
	jsonBody, err := json.Marshal(testSettings)
	s.NoError(err)
	buffer := bytes.NewBuffer(jsonBody)
	request, err := http.NewRequest("POST", "/admin/settings", buffer)
	s.NoError(err)
	s.NoError(s.postHandler.RequestHandler.ParseAndValidate(ctx, request))

	// test executing the POST request
	resp, err := s.postHandler.RequestHandler.Execute(ctx, s.sc)
	s.NoError(err)
	s.NotNil(resp)

	// test getting the settings
	s.NoError(s.getHandler.RequestHandler.ParseAndValidate(ctx, nil))
	resp, err = s.getHandler.RequestHandler.Execute(ctx, s.sc)
	s.NoError(err)
	s.NotNil(resp)
	settingsResp, err := resp.Result[0].ToService()
	s.NoError(err)
	settings, ok := settingsResp.(evergreen.Settings)
	s.True(ok)
	s.EqualValues(testSettings.Alerts.SMTP.From, settings.Alerts.SMTP.From)
	s.EqualValues(testSettings.Alerts.SMTP.Port, settings.Alerts.SMTP.Port)
	s.Equal(len(testSettings.Alerts.SMTP.AdminEmail), len(settings.Alerts.SMTP.AdminEmail))
	s.EqualValues(testSettings.Amboy.Name, settings.Amboy.Name)
	s.EqualValues(testSettings.Amboy.LocalStorage, settings.Amboy.LocalStorage)
	s.EqualValues(testSettings.Api.HttpListenAddr, settings.Api.HttpListenAddr)
	s.EqualValues(testSettings.AuthConfig.Crowd.Username, settings.AuthConfig.Crowd.Username)
	s.EqualValues(testSettings.AuthConfig.Naive.Users[0].Username, settings.AuthConfig.Naive.Users[0].Username)
	s.EqualValues(testSettings.AuthConfig.Github.ClientId, settings.AuthConfig.Github.ClientId)
	s.Equal(len(testSettings.AuthConfig.Github.Users), len(settings.AuthConfig.Github.Users))
	s.EqualValues(testSettings.HostInit.SSHTimeoutSeconds, settings.HostInit.SSHTimeoutSeconds)
	s.EqualValues(testSettings.Jira.Username, settings.Jira.Username)
	s.EqualValues(testSettings.LoggerConfig.DefaultLevel, settings.LoggerConfig.DefaultLevel)
	s.EqualValues(testSettings.LoggerConfig.Buffer.Count, settings.LoggerConfig.Buffer.Count)
	s.EqualValues(testSettings.NewRelic.ApplicationName, settings.NewRelic.ApplicationName)
	s.EqualValues(testSettings.Notify.SMTP.From, settings.Notify.SMTP.From)
	s.EqualValues(testSettings.Notify.SMTP.Port, settings.Notify.SMTP.Port)
	s.Equal(len(testSettings.Notify.SMTP.AdminEmail), len(settings.Notify.SMTP.AdminEmail))
	s.EqualValues(testSettings.Providers.AWS.Id, settings.Providers.AWS.Id)
	s.EqualValues(testSettings.Providers.Docker.APIVersion, settings.Providers.Docker.APIVersion)
	s.EqualValues(testSettings.Providers.GCE.ClientEmail, settings.Providers.GCE.ClientEmail)
	s.EqualValues(testSettings.Providers.OpenStack.IdentityEndpoint, settings.Providers.OpenStack.IdentityEndpoint)
	s.EqualValues(testSettings.Providers.VSphere.Host, settings.Providers.VSphere.Host)
	s.EqualValues(testSettings.RepoTracker.MaxConcurrentRequests, settings.RepoTracker.MaxConcurrentRequests)
	s.EqualValues(testSettings.Scheduler.TaskFinder, settings.Scheduler.TaskFinder)
	s.EqualValues(testSettings.ServiceFlags.HostinitDisabled, settings.ServiceFlags.HostinitDisabled)
	s.EqualValues(testSettings.Slack.Level, settings.Slack.Level)
	s.EqualValues(testSettings.Slack.Options.Channel, settings.Slack.Options.Channel)
	s.EqualValues(testSettings.Splunk.Channel, settings.Splunk.Channel)
	s.EqualValues(testSettings.Ui.HttpListenAddr, settings.Ui.HttpListenAddr)

	// test that invalid input errors
	testSettings.ApiUrl = ""
	testSettings.Ui.CsrfKey = "12345"
	jsonBody, err = json.Marshal(testSettings)
	s.NoError(err)
	buffer = bytes.NewBuffer(jsonBody)
	request, err = http.NewRequest("POST", "/admin", buffer)
	s.NoError(err)
	s.NoError(s.postHandler.RequestHandler.ParseAndValidate(ctx, request))
	resp, err = s.postHandler.RequestHandler.Execute(ctx, s.sc)
	s.Contains(err.Error(), "API hostname must not be empty")
	s.Contains(err.Error(), "CSRF key must be 32 characters long")
	s.NotNil(resp)
}

func (s *AdminRouteSuite) TestGetAuthentication() {
	superUser := user.DBUser{
		Id: "super_user",
	}
	normalUser := user.DBUser{
		Id: "normal_user",
	}
	s.sc.SetSuperUsers([]string{"super_user"})

	superCtx := context.WithValue(context.Background(), evergreen.RequestUser, &superUser)
	normalCtx := context.WithValue(context.Background(), evergreen.RequestUser, &normalUser)

	s.NoError(s.getHandler.Authenticate(superCtx, s.sc))
	s.Error(s.getHandler.Authenticate(normalCtx, s.sc))
}

func (s *AdminRouteSuite) TestPostAuthentication() {
	superUser := user.DBUser{
		Id: "super_user",
	}
	normalUser := user.DBUser{
		Id: "normal_user",
	}
	s.sc.SetSuperUsers([]string{"super_user"})

	superCtx := context.WithValue(context.Background(), evergreen.RequestUser, &superUser)
	normalCtx := context.WithValue(context.Background(), evergreen.RequestUser, &normalUser)

	s.NoError(s.postHandler.Authenticate(superCtx, s.sc))
	s.Error(s.postHandler.Authenticate(normalCtx, s.sc))
}

func (s *AdminRouteSuite) TestRevertRoute() {
	const route = "/admin/revert"
	const version = 2

	routeManager := getRevertRouteManager(route, version)
	user := &user.DBUser{Id: "userName"}
	ctx := context.WithValue(context.Background(), evergreen.RequestUser, user)
	s.NotNil(routeManager)
	s.Equal(route, routeManager.Route)
	s.Equal(version, routeManager.Version)
	handler := routeManager.Methods[0]
	changes := restModel.APIAdminSettings{
		SuperUsers: []string{"me"},
	}
	before := evergreen.Settings{}
	_, err := s.sc.SetEvergreenSettings(&changes, &before, user, true)
	s.NoError(err)
	dbEvents, err := event.FindAdmin(event.RecentAdminEvents(1))
	s.NoError(err)
	eventData := dbEvents[0].Data.(*event.AdminEventData)
	guid := eventData.GUID
	s.NotEmpty(guid)

	body := struct {
		GUID string `json:"guid"`
	}{guid}
	jsonBody, err := json.Marshal(&body)
	s.NoError(err)
	buffer := bytes.NewBuffer(jsonBody)
	request, err := http.NewRequest("POST", "/admin/revert", buffer)
	s.NoError(err)
	s.NoError(handler.ParseAndValidate(ctx, request))
	resp, err := handler.Execute(ctx, s.sc)
	s.NoError(err)
	s.NotNil(resp)

	body = struct {
		GUID string `json:"guid"`
	}{""}
	jsonBody, err = json.Marshal(&body)
	s.NoError(err)
	buffer = bytes.NewBuffer(jsonBody)
	request, err = http.NewRequest("POST", "/admin/revert", buffer)
	s.NoError(err)
	s.Error(handler.ParseAndValidate(ctx, request))
}

func TestRestartRoute(t *testing.T) {
	assert := assert.New(t)

	ctx := context.WithValue(context.Background(), evergreen.RequestUser, &user.DBUser{Id: "userName"})
	const route = "/admin/restart"
	const version = 2

	queue := evergreen.GetEnvironment().LocalQueue()

	routeManager := getRestartRouteManager(queue)(route, version)
	assert.NotNil(routeManager)
	assert.Equal(route, routeManager.Route)
	assert.Equal(version, routeManager.Version)
	handler := routeManager.Methods[0]
	startTime := time.Date(2017, time.June, 12, 11, 0, 0, 0, time.Local)
	endTime := time.Date(2017, time.June, 12, 13, 0, 0, 0, time.Local)

	// test that invalid time range errors
	body := struct {
		StartTime time.Time `json:"start_time"`
		EndTime   time.Time `json:"end_time"`
		DryRun    bool      `json:"dry_run"`
	}{endTime, startTime, false}
	jsonBody, err := json.Marshal(&body)
	assert.NoError(err)
	buffer := bytes.NewBuffer(jsonBody)
	request, err := http.NewRequest("POST", "/admin/restart", buffer)
	assert.NoError(err)
	assert.Error(handler.ParseAndValidate(ctx, request))

	// test a valid request
	body = struct {
		StartTime time.Time `json:"start_time"`
		EndTime   time.Time `json:"end_time"`
		DryRun    bool      `json:"dry_run"`
	}{startTime, endTime, false}
	jsonBody, err = json.Marshal(&body)
	assert.NoError(err)
	buffer = bytes.NewBuffer(jsonBody)
	request, err = http.NewRequest("POST", "/admin/restart", buffer)
	assert.NoError(err)
	assert.NoError(handler.ParseAndValidate(ctx, request))
	resp, err := handler.Execute(ctx, &data.MockConnector{})
	assert.NoError(err)
	assert.NotNil(resp)
	model, ok := resp.Result[0].(*restModel.RestartTasksResponse)
	assert.True(ok)
	assert.True(len(model.TasksRestarted) > 0)
	assert.Nil(model.TasksErrored)
}

func TestBannerRoutes(t *testing.T) {
	assert := assert.New(t)

	ctx := context.WithValue(context.Background(), evergreen.RequestUser, &user.DBUser{Id: "userName"})
	const route = "/admin/banner"
	const version = 2

	routeManager := getBannerRouteManager(route, version)
	assert.NotNil(routeManager)
	assert.Equal(route, routeManager.Route)
	assert.Equal(version, routeManager.Version)
	postHandler := routeManager.Methods[0]
	getHandler := routeManager.Methods[1]
	connector := data.MockConnector{}

	// test that invalid theme errors
	body := struct {
		Text  string `json:"banner"`
		Theme string `json:"theme"`
	}{"foo", "bar"}
	jsonBody, err := json.Marshal(&body)
	assert.NoError(err)
	buffer := bytes.NewBuffer(jsonBody)
	request, err := http.NewRequest("POST", "/admin/banner", buffer)
	assert.NoError(err)
	assert.NoError(postHandler.ParseAndValidate(ctx, request))
	_, err = postHandler.Execute(ctx, &data.MockConnector{})
	assert.Error(err)

	// test a valid post request
	body = struct {
		Text  string `json:"banner"`
		Theme string `json:"theme"`
	}{"foo", "warning"}
	jsonBody, err = json.Marshal(&body)
	assert.NoError(err)
	buffer = bytes.NewBuffer(jsonBody)
	request, err = http.NewRequest("POST", "/admin/banner", buffer)
	assert.NoError(err)
	assert.NoError(postHandler.ParseAndValidate(ctx, request))
	resp, err := postHandler.Execute(ctx, &connector)
	assert.NoError(err)
	assert.NotNil(resp)

	// test getting what we just sets
	request, err = http.NewRequest("GET", "/admin/banner", nil)
	assert.NoError(err)
	assert.NoError(getHandler.ParseAndValidate(ctx, request))
	resp, err = getHandler.Execute(ctx, &connector)
	assert.NoError(err)
	assert.NotNil(resp)
	modelInterface, err := resp.Result[0].ToService()
	assert.NoError(err)
	model := modelInterface.(*restModel.APIBanner)
	assert.EqualValues(restModel.ToAPIString("foo"), model.Text)
	assert.EqualValues(restModel.ToAPIString("warning"), model.Theme)
}

func TestAdminEventRoute(t *testing.T) {
	assert := assert.New(t)
	db.SetGlobalSessionProvider(testutil.TestConfig().SessionFactory())
	testutil.HandleTestingErr(db.ClearCollections(evergreen.ConfigCollection, event.AllLogCollection), t,
		"Error clearing collections")

	// log some changes in the event log with the /admin/settings route
	ctx := context.Background()
	ctx = context.WithValue(ctx, evergreen.RequestUser, &user.DBUser{Id: "user"})
	routeManager := getAdminSettingsManager("/admin/settings", 2)
	testSettings := testutil.MockConfig()
	jsonBody, err := json.Marshal(testSettings)
	assert.NoError(err)
	buffer := bytes.NewBuffer(jsonBody)
	request, err := http.NewRequest("POST", "/admin/settings", buffer)
	assert.NoError(err)
	assert.NoError(routeManager.Methods[1].RequestHandler.ParseAndValidate(ctx, request))
	now := time.Now()
	resp, err := routeManager.Methods[1].RequestHandler.Execute(ctx, &data.DBConnector{})
	assert.NoError(err)
	assert.NotNil(resp)

	// wait a bit before retrieving results because by default the route uses time.Now to search
	time.Sleep(1 * time.Second)

	// get the changes with the /admin/events route
	ctx = context.Background()
	routeManager = getAdminEventRouteManager("/admin/events", 2)
	request, err = http.NewRequest("GET", "/admin/settings?limit=10", nil)
	assert.NoError(err)
	assert.NoError(routeManager.Methods[0].RequestHandler.ParseAndValidate(ctx, request))
	resp, err = routeManager.Methods[0].RequestHandler.Execute(ctx, &data.DBConnector{})
	assert.NoError(err)
	assert.NotNil(resp)
	count := 0
	for _, model := range resp.Result {
		evt, ok := model.(*restModel.APIAdminEvent)
		assert.True(ok)
		count++
		assert.NotEmpty(evt.Guid)
		assert.NotNil(evt.Before)
		assert.NotNil(evt.After)
		assert.Equal("user", evt.User)
	}
	assert.Equal(10, count)
	pagination := resp.Metadata.(*PaginationMetadata)
	assert.Equal("ts", pagination.KeyQueryParam)
	assert.Equal("limit", pagination.LimitQueryParam)
	assert.NotNil(pagination.Pages.Next)
	assert.Equal("next", pagination.Pages.Next.Relation)
	assert.Equal(10, pagination.Pages.Next.Limit)
	ts, err := time.Parse(time.RFC3339, pagination.Pages.Next.Key)
	assert.NoError(err)
	assert.InDelta(now.Unix(), ts.Unix(), float64(time.Millisecond.Nanoseconds()))
}
