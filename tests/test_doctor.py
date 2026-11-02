from slashpeak.doctor import reservation_name, run_doctor
from slashpeak._core.settings import Settings
from slashpeak._core.transport import Reply


class T:
    def __init__(self, config_status=200, config=None):
        self.config_status = config_status
        self.config = config or {"policies": [{"projects": [], "mode": "active", "allowedReservations": ["admin:US.etl"]}]}

    def request(self, method, path, body=None, headers=None, timeout=None):
        if path == "/v1/health":
            return Reply(200, {"ok": True}, None)
        if self.config_status != 200:
            return Reply(self.config_status, {"error": {"code": "unauthorized", "message": "x"}}, None)
        return Reply(200, self.config, None)

    def close(self):
        pass


class Session:
    def __init__(self, granted):
        self.granted = granted
        self.urls = []

    def post(self, url, json, timeout):
        self.urls.append(url)
        granted = self.granted

        class R:
            status_code = 200

            def json(self):
                return {"permissions": ["bigquery.reservations.use"]} if granted else {}

        return R()


def run(**kw):
    lines = []
    session = kw.pop("session", Session(True))
    code = run_doctor(
        settings=kw.pop("settings", Settings(api_key="dp_live_secret", endpoint="https://x")),
        out=lines.append,
        transport=kw.pop("transport", T()),
        iam_session_factory=lambda: (session, "sa@p.iam.gserviceaccount.com"),
        **kw,
    )
    return code, "\n".join(lines), session


def test_all_good():
    code, text, session = run()
    assert code == 0, text
    assert session.urls == ["https://bigqueryreservation.googleapis.com/v1/projects/admin/locations/US/reservations/etl:testIamPermissions"]
    assert "dp_live_secret" not in text


def test_missing_permission_prints_fix():
    code, text, _ = run(session=Session(False))
    assert code == 1 and "roles/bigquery.user" in text and "jobUser" in text


def test_bad_key_and_no_key():
    code, text, _ = run(transport=T(config_status=401))
    assert code == 1 and "key refused" in text
    code, text, _ = run(settings=Settings(api_key=None))
    assert code == 1 and "SLASHPEAK_API_KEY" in text


def test_active_without_reservation():
    code, text, _ = run(transport=T(config={"policies": [{"projects": [], "mode": "active", "allowedReservations": []}]}))
    assert code == 1 and "no allowed reservation" in text


def test_reservation_name():
    assert reservation_name("admin:US.etl") == "projects/admin/locations/US/reservations/etl"
    assert reservation_name("bad") is None
