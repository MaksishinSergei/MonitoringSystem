# from cysystemd.reader import JournalReader, JournalOpenMode
import systemd.journal
import pwd, grp
import asyncio, aiohttp

SYSLOG_IDENTIFIER = ["su", "sudo", "login", "systemd-logind", "lightdm", "sshd", "useradd", "usermod", "userdel", "adduser", "deluser", "groupadd", "groupmod", "groupdel", "addgroup", "delgroup"]
SERVER_URL = "http://localhost:3001/api/logs/storage"

class MonitorAgent:
    def __init__(self):
        self.journal_obj = systemd.journal.Reader()
        self.session = None

    async def run(self):
        await self.init_session()
        try:
            await self.monitor_journal()
        except KeyboardInterrupt:
            print("Остановка мониторинга...")
        finally:
            await self.close_session()
            self.journal_obj.close()

    async def monitor_journal(self):
        self.journal_obj.seek_tail()
        self.journal_obj.get_previous()
        while True:
            await asyncio.get_event_loop().run_in_executor(None, self.journal_obj.wait, -1)
            tasks = []
            for index, record in enumerate(self.journal_obj):
                if record.get("SYSLOG_IDENTIFIER") in SYSLOG_IDENTIFIER:
                    task = asyncio.create_task(self._handle_record(index, record))
                    tasks.append(task)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def init_session(self):
        self.session = aiohttp.ClientSession()
    
    async def close_session(self):
        if self.session:
            await self.session.close()

    async def get_username(self, uid):
        try:
            result = await asyncio.get_event_loop().run_in_executor(None, pwd.getpwuid, uid)
            return result.pw_name
        except (KeyError, TypeError):
            return "unknown"
        
    async def get_group(self, gid):
        try:
            result = await asyncio.get_event_loop().run_in_executor(None, grp.getgrgid, gid)
            return result.gr_name
        except (KeyError, TypeError):
            return "unknown"
        
    async def process_log(self, index, log):
        log_to_send = {}
        for key, value in log.items():
            log_to_send[key] = str(value)
        user = await self.get_username(int(log.get("_UID")))
        group = await self.get_group(int(log.get("_GID")))
        log_to_send.update({"UserName": user, "GroupName": group})
        print(f"Обработка лога: {index} для юзера {user} | {group}. Тип данных - {type(log)}")
        return log_to_send

    async def _handle_record(self, index, record):
        try:
            log_to_send = await self.process_log(index, record)
            await self.send_to_node(index, log_to_send)
        except Exception as e:
            print(f"Ошибка обработки записи: {e}")

    async def send_to_node(self, index, log):
        if not self.session:
            return
        try:
            async with self.session.post(
                SERVER_URL,
                json=log,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    print(f"✓ Лог отправлен: {log.get('SYSLOG_IDENTIFIER')}")
                else:
                    print(f"✗ Ошибка отправки: {response.status}")
        except Exception as e:
            print(f"✗ Ошибка соединения: {e}")

async def main():
    agent_obj = MonitorAgent()
    await agent_obj.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Приложение остановлено")

