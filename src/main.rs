#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use std::{
    path::PathBuf,
    process::{Command, Stdio},
    sync::mpsc::{self, Receiver, Sender, TryRecvError},
    thread,
    time::Duration,
};

#[derive(Debug)]
enum WorkerEvent {
    Started,
    Line(String),
    Finished(Result<(), String>),
}

struct ActiveJob {
    cancel: Sender<()>,
}

struct DownloaderApp {
    url: String,
    output_dir: PathBuf,
    yt_dlp_path: String,
    status: String,
    progress: String,
    log: Vec<String>,
    events: Receiver<WorkerEvent>,
    event_tx: Sender<WorkerEvent>,
    active: Option<ActiveJob>,
}

impl Default for DownloaderApp {
    fn default() -> Self {
        let (event_tx, events) = mpsc::channel();
        Self {
            url: String::new(),
            output_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            yt_dlp_path: "yt-dlp".to_owned(),
            status: "待機中".to_owned(),
            progress: String::new(),
            log: Vec::new(),
            events,
            event_tx,
            active: None,
        }
    }
}

impl DownloaderApp {
    fn start(&mut self) {
        let url = match url::Url::parse(self.url.trim()) {
            Ok(url) if matches!(url.scheme(), "http" | "https") => url,
            _ => {
                self.status = "有効な http/https URL を入力してください".into();
                return;
            }
        };
        if !self.output_dir.is_dir() {
            self.status = "保存先フォルダーが存在しません".into();
            return;
        }
        if self.active.is_some() {
            return;
        }

        let (cancel_tx, cancel_rx) = mpsc::channel();
        let tx = self.event_tx.clone();
        let executable = self.yt_dlp_path.trim().to_owned();
        let output = self.output_dir.join("%(title)s [%(id)s].%(ext)s");
        self.status = "起動中".into();
        self.progress.clear();
        self.log.clear();
        self.active = Some(ActiveJob { cancel: cancel_tx });

        thread::spawn(move || {
            let mut child = match Command::new(&executable)
                .arg("--newline")
                .arg("--progress")
                .arg("--no-playlist")
                .arg("--output")
                .arg(output)
                .arg(url.as_str())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(error) => {
                    let message = if error.kind() == std::io::ErrorKind::NotFound {
                        format!("yt-dlp が見つかりません: {executable}")
                    } else {
                        format!("yt-dlp を起動できません: {error}")
                    };
                    let _ = tx.send(WorkerEvent::Finished(Err(message)));
                    return;
                }
            };
            let _ = tx.send(WorkerEvent::Started);
            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            let (line_tx, line_rx) = mpsc::channel::<String>();
            if let Some(stream) = stdout {
                spawn_reader(stream, line_tx.clone());
            }
            if let Some(stream) = stderr {
                spawn_reader(stream, line_tx.clone());
            }
            drop(line_tx);
            loop {
                match cancel_rx.try_recv() {
                    Ok(()) | Err(TryRecvError::Disconnected) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = tx.send(WorkerEvent::Finished(Err(
                            "ダウンロードをキャンセルしました".into(),
                        )));
                        return;
                    }
                    Err(TryRecvError::Empty) => {}
                }
                while let Ok(line) = line_rx.try_recv() {
                    let _ = tx.send(WorkerEvent::Line(line));
                }
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let _ = tx.send(WorkerEvent::Finished(if status.success() {
                            Ok(())
                        } else {
                            Err(format!(
                                "yt-dlp が終了コード {:?} で終了しました",
                                status.code()
                            ))
                        }));
                        return;
                    }
                    Ok(None) => thread::sleep(Duration::from_millis(100)),
                    Err(error) => {
                        let _ = child.kill();
                        let _ = tx.send(WorkerEvent::Finished(Err(format!(
                            "プロセス状態の取得に失敗しました: {error}"
                        ))));
                        return;
                    }
                }
            }
        });
    }

    fn cancel(&mut self) {
        if let Some(job) = &self.active {
            let _ = job.cancel.send(());
            self.status = "キャンセル中".into();
        }
    }

    fn receive_events(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            match event {
                WorkerEvent::Started => self.status = "ダウンロード中".into(),
                WorkerEvent::Line(line) => {
                    if line.contains("%") || line.contains("ETA") || line.contains(" at ") {
                        self.progress = line.clone();
                    }
                    if self.log.len() >= 200 {
                        self.log.remove(0);
                    }
                    self.log.push(line);
                }
                WorkerEvent::Finished(Ok(())) => {
                    self.status = "完了しました".into();
                    self.active = None;
                }
                WorkerEvent::Finished(Err(error)) => {
                    self.status = error;
                    self.active = None;
                }
            }
        }
    }
}

impl eframe::App for DownloaderApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.receive_events();
        ctx.request_repaint_after(Duration::from_millis(100));

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("yt-dlp 動画ダウンローダー");
            ui.add_space(8.0);
            ui.label("動画 URL");
            ui.add_enabled(
                self.active.is_none(),
                egui::TextEdit::singleline(&mut self.url).hint_text("https://example.com/video"),
            );
            ui.add_space(8.0);
            ui.label("保存先フォルダー");
            ui.horizontal(|ui| {
                ui.monospace(self.output_dir.display().to_string());
                if ui
                    .add_enabled(self.active.is_none(), egui::Button::new("参照…"))
                    .clicked()
                {
                    if let Some(path) = rfd::FileDialog::new()
                        .set_directory(&self.output_dir)
                        .pick_folder()
                    {
                        self.output_dir = path;
                    }
                }
            });
            ui.add_space(8.0);
            ui.label("yt-dlp 実行ファイル（PATH 上なら yt-dlp）");
            ui.add_enabled(
                self.active.is_none(),
                egui::TextEdit::singleline(&mut self.yt_dlp_path),
            );
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(self.active.is_none(), egui::Button::new("ダウンロード開始"))
                    .clicked()
                {
                    self.start();
                }
                if ui
                    .add_enabled(self.active.is_some(), egui::Button::new("キャンセル"))
                    .clicked()
                {
                    self.cancel();
                }
            });
            ui.separator();
            ui.strong(&self.status);
            if !self.progress.is_empty() {
                ui.label(&self.progress);
            }
            egui::CollapsingHeader::new("ログ")
                .default_open(false)
                .show(ui, |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(220.0)
                        .show(ui, |ui| {
                            for line in &self.log {
                                ui.monospace(line);
                            }
                        });
                });
        });
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "yt-dlp 動画ダウンローダー",
        options,
        Box::new(|_cc| Ok(Box::<DownloaderApp>::default())),
    )
}

fn spawn_reader<R: std::io::Read + Send + 'static>(stream: R, tx: Sender<String>) {
    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stream).lines().flatten() {
            let _ = tx.send(line);
        }
    });
}
