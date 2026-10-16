package dev.dbos.transact.conductor;

/** The Conductor client, run for the life of a launched DBOS instance. */
public interface ConductorConnection {
  /** Starts connecting to Conductor. Called once, at the end of launch. */
  void start();

  /** Disconnects from Conductor. Called once, at the start of shutdown. */
  void stop();
}
