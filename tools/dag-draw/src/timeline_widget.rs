use std::collections::BTreeSet;

use druid::kurbo::Line;
use druid::widget::prelude::*;
use druid::widget::Widget;
use druid::{Color, Rect};
use druid::{Point, Size};

use crate::app_data::*;
use crate::data::Transfer;
use crate::draw_utils::*;

const X_PADDING: f64 = 30.0;
const ROW_STEP: f64 = 20.0;
const MEMORY_HEIGHT: f64 = 100.0;

struct TimelineResourceBlock {
    start: f64,
    end: f64,
    height: u64,
    color: Color,
    selected: bool,
    task: usize,
}

impl TimelineResourceBlock {
    fn new(start: f64, end: f64, height: u64, color: Color, selected: bool, task: usize) -> Self {
        TimelineResourceBlock {
            start,
            end,
            height,
            color,
            selected,
            task,
        }
    }
}

pub struct TimelineWidget {
    timeline_left: f64,
    timeline_right: f64,
    total_time: f64,
    size: Size,
    clickable_rectangles: Vec<(Rect, usize)>,
}

impl TimelineWidget {
    pub fn new() -> Self {
        TimelineWidget {
            timeline_left: 0.,
            timeline_right: 0.,
            total_time: 0.,
            size: Size::new(0., 0.),
            clickable_rectangles: Vec::new(),
        }
    }

    fn get_time_x(&self, time: f64) -> f64 {
        time / self.total_time * (self.timeline_right - self.timeline_left) + self.timeline_left
    }

    fn draw_cores_usage(
        &mut self,
        ctx: &mut PaintCtx,
        y: f64,
        height: f64,
        usages: Vec<TimelineResourceBlock>,
        cores: u32,
    ) {
        // (time; some number for ordering events with same time; 0 for start and 1 for end; id)
        let mut events: Vec<(f64, i32, i32, usize)> = Vec::new();
        for (i, item) in usages.iter().enumerate() {
            // first all ends, then all empty tasks consecutively, then all starts
            if item.start == item.end {
                events.push((item.start, 0, 0, i));
                events.push((item.end, 0, 1, i));
            } else {
                events.push((item.start, 1, 0, i));
                events.push((item.end, -1, 1, i));
            }
        }
        events.sort_by(|a, b| {
            a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(
                // for non-empty tasks order doesn't matter at this point
                // for empty tasks:
                // if these are different tasks, then order them by id, otherwise start comes before end
                a.3.cmp(&b.3).then(a.2.cmp(&b.2)),
            )
        });
        let mut available_cores: BTreeSet<u32> = (0..cores).collect();
        let mut usage_cores: Vec<Vec<u32>> = vec![Vec::new(); usages.len()];
        let mut highlighted_borders: Vec<Rect> = Vec::new();
        for &(_, _, event_type, usage_id) in events.iter() {
            if event_type == 1 {
                for &core in usage_cores[usage_id].iter() {
                    available_cores.insert(core);
                }
            } else {
                let mut cores_segments: Vec<(u32, u32)> = Vec::new();
                let usage = &usages[usage_id];
                for _ in 0..usage.height {
                    let core = *available_cores.iter().next().unwrap();
                    available_cores.remove(&core);
                    usage_cores[usage_id].push(core);
                    if !cores_segments.is_empty() && cores_segments.last().unwrap().1 + 1 == core {
                        cores_segments.last_mut().unwrap().1 += 1;
                    } else {
                        cores_segments.push((core, core));
                    }
                }

                for &(first_core, last_core) in cores_segments.iter() {
                    let cury = y + first_core as f64 * height;
                    let current_height = (last_core - first_core + 1) as f64 * height;
                    let rect = Rect::from_points(
                        Point::new(self.get_time_x(usage.start), cury),
                        Point::new(self.get_time_x(usage.end), cury + current_height),
                    );
                    ctx.fill(rect, &usage.color);
                    self.clickable_rectangles.push((rect, usage.task));
                    if usage.selected {
                        highlighted_borders.push(rect);
                    }
                }
            }
        }
        for rect in highlighted_borders.iter() {
            ctx.stroke(rect, &Color::WHITE, 5.);
        }
    }

    fn draw_memory_usage(&mut self, ctx: &mut PaintCtx, y: f64, height: f64, usages: Vec<TimelineResourceBlock>) {
        if usages.is_empty() {
            return;
        }
        let mut events: Vec<f64> = Vec::new();
        for item in usages.iter() {
            events.push(item.start);
            events.push(item.end);
        }
        events.sort_by(|a, b| a.total_cmp(b));
        events.dedup();
        for i in 0..events.len() - 1 {
            let left = events[i];
            let right = events[i + 1];
            let mut cury = y;
            for usage in usages.iter() {
                if usage.end <= left || usage.start >= right {
                    continue;
                }
                let current_height = usage.height as f64 * height;
                let rect = Rect::from_points(
                    Point::new(self.get_time_x(left), cury),
                    Point::new(self.get_time_x(right), cury + current_height),
                );
                ctx.fill(rect, &usage.color);
                self.clickable_rectangles.push((rect, usage.task));
                cury += current_height;
            }
        }
        for i in 0..events.len() - 1 {
            let left = events[i];
            let right = events[i + 1];
            let mut cury = y;
            for usage in usages.iter() {
                if usage.end <= left || usage.start >= right {
                    continue;
                }
                let current_height = usage.height as f64 * height;
                if usage.selected {
                    ctx.stroke(
                        Rect::from_points(
                            Point::new(self.get_time_x(left), cury),
                            Point::new(self.get_time_x(right), cury + current_height),
                        ),
                        &Color::WHITE,
                        5.,
                    );
                }
                cury += current_height;
            }
        }
    }

    fn transfer_selected(&self, transfer: &Transfer, data: &AppData) -> bool {
        if data.selected_task.is_none() {
            return false;
        }
        let task = &data.graph.borrow().tasks[data.selected_task.unwrap()];
        if task.inputs.iter().any(|&x| x == transfer.data_item_id) {
            return true;
        }
        if task.outputs.iter().any(|&x| x == transfer.data_item_id) {
            return true;
        }
        false
    }
}

impl Widget<AppData> for TimelineWidget {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event, data: &mut AppData, _: &Env) {
        if let Event::MouseDown(e) = event {
            data.selected_task = None;
            for (rect, task) in self.clickable_rectangles.iter() {
                if rect.contains(e.pos) {
                    data.selected_task = Some(*task);
                    break;
                }
            }
            if let Some(task_id) = data.selected_task {
                data.selected_task_info = get_text_task_info(data, task_id);
            } else {
                data.selected_task_info = String::new();
            }
            ctx.request_paint();
        }
    }

    fn lifecycle(&mut self, _: &mut LifeCycleCtx, _: &LifeCycle, _: &AppData, _: &Env) {}
    fn update(&mut self, ctx: &mut UpdateCtx, _: &AppData, _: &AppData, _: &Env) {
        ctx.request_paint();
    }
    fn layout(&mut self, _: &mut LayoutCtx, bc: &BoxConstraints, _: &AppData, _: &Env) -> druid::Size {
        Size::new(bc.max().width, 1000000.)
    }

    fn paint(&mut self, ctx: &mut PaintCtx, data: &AppData, _: &Env) {
        let size = ctx.size();
        self.clickable_rectangles.clear();

        let timeline_left = X_PADDING + 150.;
        let timeline_right = size.width - X_PADDING;

        self.size = size;
        self.timeline_left = timeline_left;
        self.timeline_right = timeline_right;
        self.total_time = data.total_time;

        let mut y = 20.;
        for compute in data.compute.borrow().iter() {
            let y0 = y;
            paint_text(ctx, &compute.name, 25., Point::new(X_PADDING + 5., y), false, false);

            y += 35.;

            // download
            if data.timeline_downloading {
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );
                for transfer in data.transfers.borrow().iter() {
                    if transfer.to == compute.name {
                        paint_text(ctx, &transfer.name, 15., Point::new(X_PADDING + 15., y), false, false);

                        ctx.stroke(
                            Rect::from_points(
                                Point::new(X_PADDING, y),
                                Point::new(size.width - X_PADDING, y + ROW_STEP),
                            ),
                            &Color::WHITE,
                            0.1,
                        );
                        draw_download(ctx, Point::new(X_PADDING + 8., y + 10.));

                        ctx.fill(
                            Rect::from_points(
                                Point::new(self.get_time_x(transfer.start), y + 5.),
                                Point::new(self.get_time_x(transfer.end), y + ROW_STEP - 5.),
                            ),
                            if self.transfer_selected(transfer, data) {
                                &Color::WHITE
                            } else {
                                &Color::GRAY
                            },
                        );

                        y += ROW_STEP;
                    }
                }
            }

            // cores
            if data.timeline_cores {
                let mut cores = Vec::new();
                for &task_id in compute.tasks.iter() {
                    let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
                    cores.push(TimelineResourceBlock::new(
                        task_info.scheduled,
                        task_info.completed,
                        task_info.cores as u64,
                        task_info.color.clone(),
                        data.selected_task.is_some() && data.selected_task.unwrap() == task_id,
                        task_id,
                    ));
                }
                self.draw_cores_usage(ctx, y, ROW_STEP, cores, compute.cores);
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );
                paint_text(
                    ctx,
                    &format!("Cores: {}", compute.cores),
                    15.,
                    Point::new(X_PADDING + 5., y + compute.cores as f64 * ROW_STEP / 2. - 10.),
                    false,
                    false,
                );
                for _i in 0..compute.cores {
                    ctx.stroke(
                        Rect::from_points(Point::new(timeline_left, y), Point::new(timeline_right, y)),
                        &Color::WHITE,
                        0.2,
                    );
                    y += ROW_STEP;
                }
            }

            // memory
            if data.timeline_memory {
                let mut memory = Vec::new();
                for &task_id in compute.tasks.iter() {
                    let task_info = data.task_info.borrow()[task_id].as_ref().unwrap().clone();
                    memory.push(TimelineResourceBlock::new(
                        task_info.scheduled,
                        task_info.completed,
                        data.graph.borrow().tasks[task_id].memory,
                        task_info.color.clone(),
                        data.selected_task.is_some() && data.selected_task.unwrap() == task_id,
                        task_id,
                    ));
                }
                self.draw_memory_usage(ctx, y, MEMORY_HEIGHT / compute.memory as f64, memory);
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );
                paint_text(
                    ctx,
                    &format!("Memory: {}", compute.memory),
                    15.,
                    Point::new(X_PADDING + 5., y + MEMORY_HEIGHT / 2. - 10.),
                    false,
                    false,
                );
                y += MEMORY_HEIGHT;
            }

            if data.timeline_uploading {
                ctx.stroke(
                    Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                    &Color::WHITE,
                    2.,
                );

                // upload
                for transfer in data.transfers.borrow().iter() {
                    if transfer.from == compute.name {
                        paint_text(ctx, &transfer.name, 15., Point::new(X_PADDING + 15., y), false, false);

                        ctx.stroke(
                            Rect::from_points(
                                Point::new(X_PADDING, y),
                                Point::new(size.width - X_PADDING, y + ROW_STEP),
                            ),
                            &Color::WHITE,
                            0.1,
                        );
                        draw_upload(ctx, Point::new(X_PADDING + 8., y + 10.));

                        ctx.fill(
                            Rect::from_points(
                                Point::new(self.get_time_x(transfer.start), y + 5.),
                                Point::new(self.get_time_x(transfer.end), y + ROW_STEP - 5.),
                            ),
                            if self.transfer_selected(transfer, data) {
                                &Color::WHITE
                            } else {
                                &Color::GRAY
                            },
                        );

                        y += ROW_STEP;
                    }
                }
            }
            ctx.stroke(
                Line::new(Point::new(X_PADDING, y), Point::new(size.width - X_PADDING, y)),
                &Color::WHITE,
                1.,
            );

            ctx.stroke(
                Rect::from_points(Point::new(X_PADDING, y0), Point::new(size.width - X_PADDING, y)),
                &Color::WHITE,
                3.,
            );
            ctx.stroke(
                Line::new(Point::new(timeline_left, y0 + 35.), Point::new(timeline_left, y)),
                &Color::WHITE,
                2.,
            );

            y += 50.;
        }

        let time = self.get_time_x(data.slider * data.total_time);
        ctx.stroke(
            Line::new(Point::new(time, 0.), Point::new(time, y - 30.)),
            &Color::RED,
            2.,
        );
    }
}
