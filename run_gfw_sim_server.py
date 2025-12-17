# run_gfw_sim_server.py
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "gfw_sim.api.server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
