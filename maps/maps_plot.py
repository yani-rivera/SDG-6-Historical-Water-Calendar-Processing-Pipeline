"""Map drawing for Figures 3-4: 2 x 2 layout, three map panels plus a legend
panel with scale bar and north arrow. Classes and colours come from the config."""
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch, Polygon, Rectangle


def classify(values, breaks):
    """Class per value using upper-inclusive breaks.

    breaks [5, 8, 10, 15] -> <=5, (5,8], (8,10], (10,15], >15 ; NaN -> -1.
    """
    idx = np.digitize(values, breaks, right=True)
    return np.where(np.isnan(values), -1, idx)


def draw_background(ax, layers, extent):
    for lyr in layers:
        lyr["gdf"].plot(ax=ax, color=lyr.get("fill", "#eeeeee"),
                        edgecolor=lyr.get("edge", "#d0d0d0"),
                        linewidth=lyr.get("width", 0.3), zorder=0)
    ax.set_xlim(extent[0], extent[2])
    ax.set_ylim(extent[1], extent[3])


def nice_km(extent):
    """A round bar length of about a third of the map width."""
    target = (extent[2] - extent[0]) / 1000 / 3
    for v in (0.5, 1, 2, 4, 5, 8, 10, 16, 20, 25, 50):
        if v >= target:
            return v
    return 50


def scale_bar(ax, extent, km, align="left", left=0.1):
    """Black/white bar of `km` kilometres (map units in metres), top of the panel.

    align "left": starts at fraction `left` of the width; "center": centred.
    """
    if align == "center":
        x0 = extent[0] + 0.5 * (extent[2] - extent[0]) - km * 1000 / 2
    else:
        x0 = extent[0] + left * (extent[2] - extent[0])
    y0 = extent[1] + 0.85 * (extent[3] - extent[1])
    h = 0.025 * (extent[3] - extent[1])
    half = km * 1000 / 2
    ax.add_patch(Rectangle((x0, y0), half, h, facecolor="black", edgecolor="black"))
    ax.add_patch(Rectangle((x0 + half, y0), half, h, facecolor="white", edgecolor="black"))
    for dx, lab in ((0, "0"), (half, f"{km / 2:g}"), (2 * half, f"{km:g} km")):
        ax.text(x0 + dx, y0 + 1.8 * h, lab, ha="center", va="bottom", fontsize=8)


def north_arrow(ax, extent, fx=0.9, fy=0.95):
    """Arrow centred at fraction fx of the width, tip at fraction fy of the height."""
    w, h = extent[2] - extent[0], extent[3] - extent[1]
    cx, top = extent[0] + fx * w, extent[1] + fy * h
    tri = [(cx, top), (cx - 0.03 * w, top - 0.12 * h), (cx + 0.03 * w, top - 0.12 * h)]
    ax.add_patch(Polygon(tri, closed=True, facecolor="white", edgecolor="black", lw=1))
    ax.add_patch(Polygon([tri[0], tri[1], (cx, top - 0.09 * h)], closed=True,
                         facecolor="black", edgecolor="black", lw=1))
    ax.text(cx, top - 0.16 * h, "N", ha="center", va="top", fontsize=9)


def draw_map(ax, layer, variable, fig_cfg, style, background, base, extent, title):
    """One map: background, original neighborhoods as grey base, classes on top.

    Neighborhoods without a value are not coloured (they remain part of the base).
    """
    breaks, colors = fig_cfg["breaks"], fig_cfg["colors"]
    draw_background(ax, background, extent)
    if base is not None:
        base.plot(ax=ax, color=style.get("base_fill", "#e6e6e6"),
                  edgecolor=style.get("base_edge", "#f2f2f2"),
                  linewidth=style.get("base_width", 0.1), zorder=0.5)
    vals = layer.dropna(subset=[variable])
    cls = classify(vals[variable].to_numpy(dtype=float), breaks)
    vals.plot(ax=ax, color=[colors[c] for c in cls], edgecolor=style["edge_color"],
              linewidth=style["edge_width"], zorder=1)
    ax.set_xlim(extent[0], extent[2])
    ax.set_ylim(extent[1], extent[3])
    for coll in ax.collections:          # nothing drawn outside the map frame
        coll.set_clip_on(True)
    ax.set_title(title, fontsize=10, loc="left", fontweight="bold")
    ax.set_axis_off()
    ax.set_aspect("equal")


def legend_handles(fig_cfg, style):
    handles = [Patch(facecolor=c, edgecolor="#999999", label=l)
               for c, l in zip(fig_cfg["colors"], fig_cfg["labels"])]
    if style.get("show_no_data", False):
        handles.append(Patch(facecolor=style.get("base_fill", "#e6e6e6"), edgecolor="#999999",
                             label=style["no_data_label"]))
    return handles


def draw_single(layer, title, variable, fig_cfg, style, background, base, extent):
    """One map as its own figure, with legend, scale bar and north arrow."""
    fig, ax = plt.subplots(figsize=(9.5, 7))
    margin = style.get("single_map_right_margin", 0.12)
    wide = (extent[0], extent[1], extent[2] + margin * (extent[2] - extent[0]), extent[3])
    draw_map(ax, layer, variable, fig_cfg, style, background, base, wide, title)
    north_arrow(ax, wide, fx=1 - margin / (2 * (1 + margin)), fy=0.97)
    # legend outside the map, at the lower right, so no data is covered
    ax.legend(handles=legend_handles(fig_cfg, style), title=fig_cfg["legend_title"],
              loc="lower left", bbox_to_anchor=(1.01, 0.0), fontsize=8, title_fontsize=8,
              frameon=False)
    fig.tight_layout()
    return fig


def draw_figure(layers, titles, variable, fig_cfg, style, background, extent, base=None):
    breaks, colors, labels = fig_cfg["breaks"], fig_cfg["colors"], fig_cfg["labels"]
    if not (len(colors) == len(labels) == len(breaks) + 1):
        raise SystemExit(f"{fig_cfg['name']}: need len(colors) == len(labels) == len(breaks) + 1")

    fig, axes = plt.subplots(2, 2, figsize=(11, 9.5))
    # grid positions [row, col] of the map panels, in panel order; legend in the free cell
    pos = style.get("panel_positions", [[0, 0], [0, 1], [1, 0]])
    map_axes = [axes[r, c] for r, c in pos]
    for ax, layer, title in zip(map_axes, layers, titles):
        draw_map(ax, layer, variable, fig_cfg, style, background, base, extent, title)

    free = [(r, c) for r in range(2) for c in range(2) if [r, c] not in pos][0]
    leg_ax = axes[free]
    leg_ax.set_xlim(extent[0], extent[2])
    leg_ax.set_ylim(extent[1], extent[3])
    leg_ax.set_aspect("equal")
    leg_ax.set_axis_off()
    align = style.get("legend_align", "left")
    left = style.get("legend_left", 0.1)
    scale_bar(leg_ax, extent, style.get("scale_bar_km") or nice_km(extent), align, left)
    north_arrow(leg_ax, extent, fx=0.88, fy=0.97)
    if align == "center":
        loc, anchor = "upper center", (0.5, 0.72)
    else:
        loc, anchor = "upper left", (left - 0.02, 0.72)
    leg = leg_ax.legend(handles=legend_handles(fig_cfg, style), title=fig_cfg["legend_title"],
                        loc=loc, bbox_to_anchor=anchor, fontsize=9, title_fontsize=9,
                        frameon=False, alignment=align)
    fig.tight_layout(rect=(0, 0, 1, style.get("title_space", 0.93)),
                     w_pad=style.get("panel_spacing", 3.0), h_pad=style.get("panel_spacing", 3.0))
    # main title aligned with the left edge of the first map panel
    fig.suptitle(fig_cfg["title"], fontsize=13, fontweight="bold",
                 x=map_axes[0].get_position().x0, ha="left")
    return fig
