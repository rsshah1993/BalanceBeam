mod request;
mod response;

use clap::Clap;
use crossbeam_channel;
use crossbeam_channel::Sender;
use rand::{Rng, SeedableRng};
use std::time::Instant;
use tokio::task;
use tokio::{
    net::{TcpListener, TcpStream},
    stream::StreamExt,
};

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Clap, Debug, Clone)]
#[clap(about = "Fun with load balancing")]
struct CmdOptions {
    #[clap(
        short,
        long,
        about = "IP/port to bind to",
        default_value = "0.0.0.0:1100"
    )]
    bind: String,
    #[clap(short, long, about = "Upstream host to forward requests to")]
    upstream: Vec<String>,
    #[clap(
        long,
        about = "Perform active health checks on this interval (in seconds)",
        default_value = "10"
    )]
    active_health_check_interval: usize,
    #[clap(
        long,
        about = "Path to send request to for active health checks",
        default_value = "/"
    )]
    active_health_check_path: String,
    #[clap(
        long,
        about = "Maximum number of requests to accept per IP per minute (0 = unlimited)",
        default_value = "0"
    )]
    max_requests_per_minute: usize,
    #[clap(
        long,
        about = "Number of threads to run BalanceBeam on.",
        default_value = "10"
    )]
    num_threads: usize,
    #[clap(long, about = "Run with multiple threads")]
    run_threaded: bool,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
#[derive(Clone)]
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
    /// dead servers, we can continue to check these and put them back in rotation
    /// once they return 200 from our active health checks
    dead_upstream_addresses: Vec<String>,
}
#[tokio::main]
async fn main() {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let mut listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections
    let mut state = ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        dead_upstream_addresses: Vec::new(),
    };

    // channels to communicate failover for passive health checks
    let (sender, receiver) = crossbeam_channel::unbounded();
    let (alive_sender, alive_receiver) = crossbeam_channel::unbounded();
    let (dead_sender, dead_receiver) = crossbeam_channel::unbounded();

    let state_clone = state.clone();
    task::spawn(async move { do_health_checks(state_clone, alive_sender, dead_sender) });

    while let Some(stream) = listener.next().await {
        if let Ok(stream) = stream {
            let state = state.clone();
            let sender = sender.clone();
            task::spawn(async move {
                handle_connection(stream, state, sender).await;
            });
        }
        // passive health checks
        if let Ok(dead_ip) = receiver.try_recv() {
            log::debug!("Received dead IP index: {}", dead_ip);
            println!("Upstream Addresses: {:?}", state.upstream_addresses);
            let index = state.upstream_addresses.iter().position(|x| *x == dead_ip);
            // push into dead addresses to check again
            state.dead_upstream_addresses.push(dead_ip);
            match index {
                Some(index) => {
                    state.upstream_addresses.remove(index);
                }
                None => (),
            }
        }
        // active health checks
        if let Ok(dead_ip) = alive_receiver.try_recv() {
            log::debug!(
                "Received dead IP from active health checks index: {}",
                dead_ip
            );
            let index = state.upstream_addresses.iter().position(|x| *x == dead_ip);
            // push into dead addresses to check again
            state.dead_upstream_addresses.push(dead_ip);
            match index {
                Some(index) => {
                    state.upstream_addresses.remove(index);
                }
                None => (),
            }
        }

        if let Ok(alive_ip) = dead_receiver.try_recv() {
            log::debug!("Dead IP: {} back online", alive_ip);
            let index = state
                .dead_upstream_addresses
                .iter()
                .position(|x| *x == alive_ip);
            // push into dead addresses to check again
            state.upstream_addresses.push(alive_ip);
            match index {
                Some(index) => {
                    state.dead_upstream_addresses.remove(index);
                }
                None => (),
            }
        }
    }
}

async fn do_health_checks(
    mut state: ProxyState,
    alive_sender: Sender<String>,
    dead_sender: Sender<String>,
) {
    log::debug!("Starting health checker");
    // start timer for health check
    let mut now = Instant::now();
    loop {
        if now.elapsed().as_secs() >= state.active_health_check_interval as u64 {
            let mut active2dead = Vec::new();
            let mut dead2active = Vec::new();
            log::debug!("Running health check!");

            // check active servers
            for (i, server) in state.upstream_addresses.iter().enumerate() {
                let mut stream = match TcpStream::connect(server).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        log::error!("Could not bind to {}: {}", server, err);
                        std::process::exit(1);
                    }
                };
                if let Err(_health) =
                    health_check(&mut stream, server, &state.active_health_check_path).await
                {
                    state.dead_upstream_addresses.push(server.clone());
                    active2dead.push(i);
                    alive_sender.send(server.clone()).unwrap();
                }
            }
            for i in active2dead {
                state.upstream_addresses.remove(i);
            }
            // check dead servers
            for (i, server) in state.dead_upstream_addresses.iter().enumerate() {
                let mut stream = match TcpStream::connect(server).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        log::error!("Could not bind to {}: {}", server, err);
                        std::process::exit(1);
                    }
                };
                if let Ok(_health) =
                    health_check(&mut stream, server, &state.active_health_check_path).await
                {
                    state.upstream_addresses.push(server.clone());
                    dead2active.push(i);
                    dead_sender.send(server.clone()).unwrap();
                }
            }
            for i in dead2active {
                state.dead_upstream_addresses.remove(i);
            }
            now = Instant::now();
        }
    }
}

async fn connect_to_upstream(
    state: &mut ProxyState,
    sender: Sender<String>,
) -> Result<TcpStream, std::io::Error> {
    log::info!("State upstreams per thread: {:?}", state.upstream_addresses);

    let mut rng = rand::rngs::StdRng::from_entropy();
    let upstream_idx = rng.gen_range(0, state.upstream_addresses.len());
    let upstream_ip = { &state.upstream_addresses[upstream_idx] };
    let connection = TcpStream::connect(upstream_ip).await;
    match connection {
        Ok(connection) => Ok(connection),
        Err(error) => {
            log::error!("Failed to connect to upstream {}: {}", upstream_ip, error);
            let dead_ip_clone = upstream_ip.clone();
            state.upstream_addresses.remove(upstream_idx);
            sender.send(dead_ip_clone).unwrap();
            Err(error)
        }
    }
}

async fn health_check(
    client_conn: &mut TcpStream,
    upstream: &String,
    path: &String,
) -> Result<(), std::io::Error> {
    // create request
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(path)
        .header("Host", upstream)
        .body(Vec::new())
        .unwrap();
    request::write_to_stream(&request, client_conn).await
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

async fn handle_connection(
    mut client_conn: TcpStream,
    mut state: ProxyState,
    sender: Sender<String>,
) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let upstream_conn = loop {
        let stream = connect_to_upstream(&mut state, sender.clone()).await;
        match stream {
            Ok(stream) => break Ok(stream),
            Err(_error) => {
                if state.upstream_addresses.is_empty() {
                    let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                    send_response(&mut client_conn, &response).await;
                    break Err(_error);
                } else {
                    continue;
                }
            }
        };
    };
    let mut upstream_conn = upstream_conn.unwrap();

    let upstream_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::UpstreamServerUnavailable
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()).await
        {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response).await;
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}
