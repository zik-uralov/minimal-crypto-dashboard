#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from config import KAFKA_CONFIG, TOPICS


def ensure_topics():
    """Create Kafka topics defined in config if they do not exist."""
    admin = AdminClient({'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'][0]})
    try:
        metadata = admin.list_topics(timeout=5)
    except Exception as exc:
        print(f"⚠️ Unable to list topics ({exc}); topics may not be created automatically.")
        return

    existing = set(metadata.topics.keys())
    desired = set(TOPICS.values())
    missing = desired - existing

    if not missing:
        return

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in missing]
    futures = admin.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"📦 Created Kafka topic: {topic}")
        except Exception as exc:
            if "Topic already exists" in str(exc):
                continue
            print(f"⚠️ Failed to create topic {topic}: {exc}")

def main():
    print("🚀 Starting Minimal Crypto Analytics Dashboard...")
    print("=" * 50)
    
    processes = []
    project_root = os.path.dirname(os.path.abspath(__file__))

    def with_project_env():
        """Clone environment and ensure project root is on PYTHONPATH."""
        env = os.environ.copy()
        existing = env.get("PYTHONPATH", "")
        if project_root not in existing.split(os.pathsep):
            env["PYTHONPATH"] = (
                f"{project_root}{os.pathsep}{existing}" if existing else project_root
            )
        return env

    ensure_topics()
    
    try:
        # Start data generator
        print("📊 Starting data generator...")
        proc_gen = subprocess.Popen(
            [sys.executable, "-m", "producers.data_generator"],
            env=with_project_env(),
        )
        processes.append(proc_gen)
        time.sleep(2)
        
        # Start metrics calculator
        print("🧮 Starting metrics calculator...")
        proc_metrics = subprocess.Popen(
            [sys.executable, "-m", "consumers.metrics_calculator"],
            env=with_project_env(),
        )
        processes.append(proc_metrics)
        time.sleep(2)
        
        # Start Streamlit dashboard
        print("🖥️ Starting Streamlit dashboard...")
        streamlit_cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "dashboard/app.py",
            "--server.port",
            "8501",
            "--server.headless",
            "true",
        ]
        proc_dashboard = subprocess.Popen(streamlit_cmd, env=with_project_env())
        processes.append(proc_dashboard)
        time.sleep(3)

        print("✅ All services started!")
        print("\n🌐 Access Points:")
        print("   - Dashboard: http://localhost:8501")
        print("   - Kafka UI: http://localhost:8080")
        print("   - Kafka Broker: localhost:29092")
        print("\n🛑 Press Ctrl+C to stop all services")
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down services...")
        
        # Terminate all processes
        for proc in processes:
            proc.terminate()
        
        # Wait for processes to terminate
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        print("✅ All services stopped!")
        sys.exit(0)

if __name__ == "__main__":
    main()
